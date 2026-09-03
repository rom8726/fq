package replication

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/database/storage/format"
)

func (s *Slave) synchronizeDump(ctx context.Context) error {
	request := NewDumpRequest(s.secret.Reveal(), s.sessionUUID, s.dumpLastSegmentNumber, format.SupportedCodecs())

	requestData, err := Encode(&request)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}

	responseData, err := s.client.SendRaw(ctx, requestData)
	if err != nil {
		// Check if it's a network error requiring reconnection
		if s.isNetworkError(err) {
			s.logger.Warn().
				Err(err).
				Str("session_uuid", s.sessionUUID).
				Uint64("last_segment_number", s.dumpLastSegmentNumber).
				Msg("network error detected during dump sync, attempting reconnection")
			if reconnectErr := s.reconnect(ctx); reconnectErr != nil {
				return fmt.Errorf("reconnection failed: %w", reconnectErr)
			}
			// Retry after reconnection
			responseData, err = s.client.SendRaw(ctx, requestData)
			if err != nil {
				return fmt.Errorf("send request after reconnection: %w", err)
			}
		} else {
			return fmt.Errorf("send request: %w", err)
		}
	}

	var response DumpResponse
	if err = Decode(&response, responseData); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	if response.Succeed {
		elems, err := decodeDumpBatch(response)
		if err != nil {
			return fmt.Errorf("decode dump response: %w", err)
		}

		wasReadingDump := s.readDump
		endOfDump := response.EndOfDump
		var applied chan error
		if wasReadingDump && endOfDump {
			applied = make(chan error, 1)
		}

		chunk := database.DumpChunk{
			Elems:   elems,
			Applied: applied,
		}
		if err := s.sendToDumpStream(ctx, chunk); err != nil {
			return fmt.Errorf("failed to send dump data to stream: %w", err)
		}

		if len(elems) > 0 {
			s.dumpLastSegmentNumber = maxLSN(elems)
		}

		if wasReadingDump && endOfDump {
			s.logger.Info().
				Str("session_uuid", s.sessionUUID).
				Uint64("last_segment_number", s.dumpLastSegmentNumber).
				Int("last_batch_size", len(elems)).
				Msg("dump synchronization completed, waiting for engine to apply")

			if err := s.waitForDumpChunkApplied(ctx, applied); err != nil {
				return fmt.Errorf("wait for dump apply: %w", err)
			}

			if s.dumpLastSegmentNumber > s.lastAppliedLSN {
				s.lastAppliedLSN = s.dumpLastSegmentNumber
			}
			s.readDump = false
			s.markDumpApplied()
		} else {
			s.readDump = !endOfDump
		}
		s.refreshStatus(true)

		return nil
	}

	return s.recordMasterError(response.ErrorCode, "dump")
}

const dumpReplicationMaxBatchSize = 100 * 1024 * 1024

func decodeDumpBatch(response DumpResponse) ([]database.DumpElem, error) {
	if response.BatchData == nil {
		return response.SegmentData, nil
	}

	if format.PayloadCodec(response.BatchData) != format.CodecID(response.BatchCodec) {
		return nil, fmt.Errorf(
			"dump batch codec mismatch: response %d, payload %d",
			response.BatchCodec,
			format.PayloadCodec(response.BatchData),
		)
	}

	decoded, err := format.DecodePayload(
		nil,
		response.BatchData,
		format.PayloadVersionCompressed,
		dumpReplicationMaxBatchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("decode dump batch: %w", err)
	}

	if len(decoded) == 0 {
		return nil, nil
	}

	var elems []database.DumpElem
	if err := gob.NewDecoder(bytes.NewReader(decoded)).Decode(&elems); err != nil {
		return nil, fmt.Errorf("decode dump elems: %w", err)
	}

	return elems, nil
}

func maxLSN(elems []database.DumpElem) uint64 {
	res := uint64(0)
	for _, e := range elems {
		if uint64(e.Tx) > res {
			res = uint64(e.Tx)
		}
	}

	return res
}

func (s *Slave) waitForDumpChunkApplied(ctx context.Context, applied <-chan error) error {
	if applied == nil {
		return nil
	}

	select {
	case err := <-applied:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closeCh:
		return errSlaveClosed
	}
}

// sendToDumpStream safely sends data to dumpStream with closed channel handling
//
//nolint:dupl // ok
func (s *Slave) sendToDumpStream(ctx context.Context, chunk database.DumpChunk) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error().Interface("panic", r).Msg("panic sending to dumpStream (channel closed)")
			err = fmt.Errorf("send to dumpStream: %v", r)
		}
	}()

	select {
	case s.dumpStream <- chunk:
		return nil
	default:
	}

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case s.dumpStream <- chunk:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closeCh:
		return errSlaveClosed
	case <-timer.C:
		return fmt.Errorf("timeout sending to dumpStream")
	}
}
