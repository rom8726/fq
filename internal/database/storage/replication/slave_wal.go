package replication

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"fq/internal/database/storage/wal"
)

func (s *Slave) synchronizeWAL(ctx context.Context) error {
	request := NewWALRequest(s.lastSegmentName)

	requestData, err := Encode(&request)
	if err != nil {
		return fmt.Errorf("encode wal request: %w", err)
	}

	responseData, err := s.client.Send(ctx, requestData)
	if err != nil {
		// Check if it's a network error requiring reconnection
		if s.isNetworkError(err) {
			s.logger.Warn().
				Err(err).
				Str("last_segment_name", s.lastSegmentName).
				Uint64("dump_last_segment_number", s.dumpLastSegmentNumber).
				Msg("network error detected during WAL sync, attempting reconnection")
			if reconnectErr := s.reconnect(ctx); reconnectErr != nil {
				return fmt.Errorf("reconnection failed: %w", reconnectErr)
			}
			// Retry after reconnection
			responseData, err = s.client.Send(ctx, requestData)
			if err != nil {
				return fmt.Errorf("send wal request after reconnection: %w", err)
			}
		} else {
			return fmt.Errorf("send wal request: %w", err)
		}
	}

	var response WALResponse
	if err = Decode(&response, responseData); err != nil {
		return fmt.Errorf("decode wal response: %w", err)
	}

	if response.Succeed {
		err = s.handleResponse(ctx, response)
		if err != nil {
			return fmt.Errorf("handle wal response: %w", err)
		}

		return nil
	}

	return fmt.Errorf("failed to apply replication data: master error")
}

func (s *Slave) handleResponse(ctx context.Context, response WALResponse) error {
	if response.SegmentName == "" {
		s.logger.Debug().
			Str("last_segment_name", s.lastSegmentName).
			Msg("no new WAL segments from replication")

		return nil
	}

	filename := response.SegmentName

	if err := s.saveWALSegment(filename, response.SegmentData); err != nil {
		return fmt.Errorf("save wal segment: %w", err)
	}

	if err := s.applyDataToEngine(ctx, response.SegmentData, response.SegmentName); err != nil {
		return fmt.Errorf("apply data to engine segment: %w", err)
	}

	s.lastSegmentName = response.SegmentName

	return nil
}

func (s *Slave) saveWALSegment(segmentName string, segmentData []byte) error {
	flags := os.O_CREATE | os.O_WRONLY
	filename := filepath.Join(s.walDirectory, segmentName)
	segment, err := os.OpenFile(filename, flags, 0o644)
	if err != nil {
		return fmt.Errorf("failed to create wal segment: %w", err)
	}

	if _, err = segment.Write(segmentData); err != nil {
		return fmt.Errorf("failed to write data to segment: %w", err)
	}

	return segment.Sync()
}

// sendToWALStream safely sends data to walStream with closed channel handling
func (s *Slave) sendToWALStream(logs []*wal.LogData) error {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error().Interface("panic", r).Msg("panic sending to walStream (channel closed)")
		}
	}()

	select {
	case s.walStream <- logs:
		return nil
	default:
		// Channel is full, try to send with timeout
		select {
		case s.walStream <- logs:
			return nil
		case <-time.After(5 * time.Second):
			return fmt.Errorf("timeout sending to walStream")
		}
	}
}

func (s *Slave) applyDataToEngine(ctx context.Context, segmentData []byte, segmentName string) error {
	logs, err := s.walReader.ReadSegmentData(ctx, segmentData)
	if err != nil {
		return err
	}

	sort.Slice(logs, func(i, j int) bool {
		return logs[i].LSN < logs[j].LSN
	})

	idx := len(logs)
	for i, log := range logs {
		if log.LSN <= s.dumpLastSegmentNumber {
			continue
		}

		idx = i

		break
	}

	if idx == len(logs) {
		s.logger.Debug().
			Str("segment_name", segmentName).
			Uint64("dump_last_segment_number", s.dumpLastSegmentNumber).
			Int("total_logs", len(logs)).
			Msg("skipping replicated segment, all logs already in dump")

		return nil
	}

	// Safe channel send with closed channel check
	if err := s.sendToWALStream(logs[idx:]); err != nil {
		return fmt.Errorf("failed to send WAL data to stream: %w", err)
	}

	return nil
}
