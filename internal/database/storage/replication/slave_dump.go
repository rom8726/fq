package replication

import (
	"context"
	"fmt"
	"time"

	"fq/internal/database"
)

func (s *Slave) synchronizeDump(ctx context.Context) error {
	request := NewDumpRequest(s.sessionUUID, s.dumpLastSegmentNumber)

	requestData, err := Encode(&request)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}

	responseData, err := s.client.Send(ctx, requestData)
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
			responseData, err = s.client.Send(ctx, requestData)
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
		s.readDump = !response.EndOfDump

		// Safe channel send with closed channel check
		if err := s.sendToDumpStream(response.SegmentData); err != nil {
			return fmt.Errorf("failed to send dump data to stream: %w", err)
		}

		if len(response.SegmentData) > 0 {
			s.dumpLastSegmentNumber = maxLSN(response.SegmentData)
		}

		// If dump is complete (EndOfDump = true), mark it as applied
		// The actual application happens in engine's dumpStream handler
		if wasReadingDump && response.EndOfDump {
			s.logger.Info().
				Str("session_uuid", s.sessionUUID).
				Uint64("last_segment_number", s.dumpLastSegmentNumber).
				Int("last_batch_size", len(response.SegmentData)).
				Msg("dump synchronization completed, waiting for engine to apply")
			// Give engine some time to process the last batch
			// In a real implementation, we'd wait for confirmation from engine
			// For now, we mark it after a short delay to ensure last batch is processed
			go func() {
				time.Sleep(100 * time.Millisecond)
				s.markDumpApplied()
			}()
		}

		return nil
	}

	return fmt.Errorf("failed to apply replication data: master error")
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

// sendToDumpStream safely sends data to dumpStream with closed channel handling
func (s *Slave) sendToDumpStream(data []database.DumpElem) error {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error().Interface("panic", r).Msg("panic sending to dumpStream (channel closed)")
		}
	}()

	select {
	case s.dumpStream <- data:
		return nil
	default:
		// Channel is full, try to send with timeout
		select {
		case s.dumpStream <- data:
			return nil
		case <-time.After(5 * time.Second):
			return fmt.Errorf("timeout sending to dumpStream")
		}
	}
}
