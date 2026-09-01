package replication

import (
	"context"
	"fmt"
	"time"

	"github.com/fq-db/fq/internal/database"
)

func (s *Slave) synchronizeDump(ctx context.Context) error {
	request := NewDumpRequest(s.secret.Reveal(), s.sessionUUID, s.dumpLastSegmentNumber)

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
		wasReadingDump := s.readDump
		endOfDump := response.EndOfDump
		var applied chan error
		if wasReadingDump && endOfDump {
			applied = make(chan error, 1)
		}

		chunk := database.DumpChunk{
			Elems:   response.SegmentData,
			Applied: applied,
		}
		if err := s.sendToDumpStream(ctx, chunk); err != nil {
			return fmt.Errorf("failed to send dump data to stream: %w", err)
		}

		if len(response.SegmentData) > 0 {
			s.dumpLastSegmentNumber = maxLSN(response.SegmentData)
		}

		if wasReadingDump && endOfDump {
			s.logger.Info().
				Str("session_uuid", s.sessionUUID).
				Uint64("last_segment_number", s.dumpLastSegmentNumber).
				Int("last_batch_size", len(response.SegmentData)).
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
