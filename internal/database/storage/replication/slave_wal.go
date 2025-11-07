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
			Uint64("dump_last_segment_number", s.dumpLastSegmentNumber).
			Msg("no new WAL segments from replication")

		return nil
	}

	filename := response.SegmentName
	isSameSegment := filename == s.lastSegmentName
	segmentSize := int64(len(response.SegmentData))

	// If it's the same segment, check if it has new data
	if isSameSegment && segmentSize <= s.lastSegmentSize {
		s.logger.Debug().
			Str("segment_name", filename).
			Int64("segment_size", segmentSize).
			Int64("last_segment_size", s.lastSegmentSize).
			Msg("segment has no new data, skipping")
		return nil
	}

	s.logger.Info().
		Str("segment_name", filename).
		Int64("segment_size", segmentSize).
		Str("last_segment_name", s.lastSegmentName).
		Int64("last_segment_size", s.lastSegmentSize).
		Uint64("dump_last_segment_number", s.dumpLastSegmentNumber).
		Bool("is_same_segment", isSameSegment).
		Msg("received WAL segment from master")

	// Save segment (overwrite if same segment with new data)
	if err := s.saveWALSegment(filename, response.SegmentData); err != nil {
		return fmt.Errorf("save wal segment: %w", err)
	}

	// Apply only new logs (filter by LSN)
	if err := s.applyDataToEngine(ctx, response.SegmentData, response.SegmentName); err != nil {
		return fmt.Errorf("apply data to engine segment: %w", err)
	}

	// Update last segment name and size
	s.lastSegmentName = response.SegmentName
	s.lastSegmentSize = segmentSize

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
	if len(segmentData) == 0 {
		s.logger.Warn().Str("segment_name", segmentName).Msg("received empty segment data, skipping")
		return nil
	}

	logs, err := s.walReader.ReadSegmentData(ctx, segmentData)
	if err != nil {
		return err
	}

	if len(logs) == 0 {
		s.logger.Debug().
			Str("segment_name", segmentName).
			Msg("segment contains no logs")
		return nil
	}

	sort.Slice(logs, func(i, j int) bool {
		return logs[i].LSN < logs[j].LSN
	})

	idx := len(logs)
	for i, log := range logs {
		// Skip logs that are already in dump or already applied
		if log.LSN <= s.dumpLastSegmentNumber || log.LSN <= s.lastAppliedLSN {
			continue
		}

		idx = i

		break
	}

	if idx == len(logs) {
		s.logger.Debug().
			Str("segment_name", segmentName).
			Uint64("dump_last_segment_number", s.dumpLastSegmentNumber).
			Uint64("last_applied_lsn", s.lastAppliedLSN).
			Uint64("first_log_lsn", logs[0].LSN).
			Uint64("last_log_lsn", logs[len(logs)-1].LSN).
			Int("total_logs", len(logs)).
			Msg("skipping replicated segment, all logs already applied")

		return nil
	}

	logsToApply := logs[idx:]
	lastLSN := logsToApply[len(logsToApply)-1].LSN

	s.logger.Info().
		Str("segment_name", segmentName).
		Uint64("dump_last_segment_number", s.dumpLastSegmentNumber).
		Uint64("last_applied_lsn", s.lastAppliedLSN).
		Uint64("first_log_lsn", logsToApply[0].LSN).
		Uint64("last_log_lsn", lastLSN).
		Int("total_logs", len(logs)).
		Int("logs_to_apply", len(logsToApply)).
		Msg("applying WAL logs to engine")

	// Safe channel send with closed channel check
	if err := s.sendToWALStream(logsToApply); err != nil {
		return fmt.Errorf("failed to send WAL data to stream: %w", err)
	}

	// Update last applied LSN
	s.lastAppliedLSN = lastLSN

	return nil
}
