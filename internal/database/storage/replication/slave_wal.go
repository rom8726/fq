package replication

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"fq/internal/database/storage/wal"
	"fq/internal/observability"
)

const walDirectoryPerm = 0o750

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

	s.logger.Debug().
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
	filename, err := s.walSegmentPath(segmentName)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(s.walDirectory, walDirectoryPerm); err != nil {
		return fmt.Errorf("failed to create wal directory: %w", err)
	}

	tempFile, err := os.CreateTemp(s.walDirectory, "."+segmentName+".*.tmp")
	if err != nil {
		return fmt.Errorf("failed to create temporary wal segment: %w", err)
	}
	tempName := tempFile.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tempName)
		}
	}()

	if err := writeAll(tempFile, segmentData); err != nil {
		_ = tempFile.Close()

		return fmt.Errorf("failed to write data to segment: %w", err)
	}

	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()

		return fmt.Errorf("failed to sync temporary wal segment: %w", err)
	}

	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close temporary wal segment: %w", err)
	}

	if err := os.Rename(tempName, filename); err != nil {
		return fmt.Errorf("failed to replace wal segment: %w", err)
	}
	removeTemp = false

	if err := syncDirectory(s.walDirectory); err != nil {
		return fmt.Errorf("failed to sync wal directory: %w", err)
	}

	return nil
}

func (s *Slave) walSegmentPath(segmentName string) (string, error) {
	if segmentName == "" ||
		strings.ContainsAny(segmentName, `/\`) ||
		segmentName != filepath.Clean(segmentName) {
		return "", fmt.Errorf("invalid wal segment name: %q", segmentName)
	}

	return filepath.Join(s.walDirectory, segmentName), nil
}

func writeAll(file *os.File, data []byte) error {
	for len(data) > 0 {
		written, err := file.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}

		data = data[written:]
	}

	return nil
}

func syncDirectory(directory string) error {
	dir, err := os.Open(directory)
	if err != nil {
		return err
	}

	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return syncErr
	}

	return closeErr
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

	// Sort logs by LSN if needed (usually they are already sorted)
	if len(logs) > 1 {
		sort.Slice(logs, func(i, j int) bool {
			return logs[i].LSN < logs[j].LSN
		})
	}

	// Find first log to apply
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

	// Reuse existing slice, no need to allocate new one
	logsToApply := logs[idx:]
	lastLSN := logsToApply[len(logsToApply)-1].LSN
	observability.SetReplicationLagLSN(lastLSN - s.lastAppliedLSN)

	s.logger.Debug().
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
	observability.SetReplicationLagLSN(0)

	return nil
}
