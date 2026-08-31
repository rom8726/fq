package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/fq-db/fq/internal/database/storage/wal"
	"github.com/fq-db/fq/internal/observability"
)

const walDirectoryPerm = 0o750

type walSegmentOffsetMismatchError struct {
	segmentName string
	size        int64
	offset      int64
}

func (e *walSegmentOffsetMismatchError) Error() string {
	return fmt.Sprintf(
		"wal segment %s is smaller than requested offset: %d < %d",
		e.segmentName,
		e.size,
		e.offset,
	)
}

func (s *Slave) synchronizeWAL(ctx context.Context) error {
	request := NewWALRequest(s.secret.Reveal(), s.replicaID, s.lastSegmentName, s.lastSegmentOffset, s.lastAppliedLSN)

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
				Str("replica_id", s.replicaID).
				Str("last_segment_name", s.lastSegmentName).
				Int64("last_segment_offset", s.lastSegmentOffset).
				Uint64("last_applied_lsn", s.lastAppliedLSN).
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
	if len(response.SegmentData) == 0 {
		return nil
	}

	s.logger.Debug().
		Str("segment_name", filename).
		Int64("segment_offset", response.SegmentOffset).
		Int64("next_segment_offset", response.NextSegmentOffset).
		Int("chunk_size", len(response.SegmentData)).
		Str("last_segment_name", s.lastSegmentName).
		Int64("last_segment_offset", s.lastSegmentOffset).
		Uint64("dump_last_segment_number", s.dumpLastSegmentNumber).
		Msg("received WAL chunk from master")

	if err := s.saveWALChunk(filename, response.SegmentOffset, response.SegmentData); err != nil {
		var offsetMismatchErr *walSegmentOffsetMismatchError
		if errors.As(err, &offsetMismatchErr) {
			s.rewindWALCursor(offsetMismatchErr)
		}

		return fmt.Errorf("save wal chunk: %w", err)
	}

	if err := s.applyDataToEngine(
		ctx,
		response.SegmentData,
		response.SegmentName,
		response.SegmentOffset == 0,
	); err != nil {
		return fmt.Errorf("apply data to engine chunk: %w", err)
	}

	s.lastSegmentName = response.SegmentName
	s.lastSegmentOffset = response.NextSegmentOffset
	s.refreshStatus(true)

	return nil
}

func (s *Slave) saveWALChunk(segmentName string, offset int64, segmentData []byte) error {
	if offset < 0 {
		return fmt.Errorf("invalid wal segment offset: %d", offset)
	}

	filename, err := s.walSegmentPath(segmentName)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(s.walDirectory, walDirectoryPerm); err != nil {
		return fmt.Errorf("failed to create wal directory: %w", err)
	}

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("failed to open wal segment: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		_ = file.Close()

		return fmt.Errorf("failed to stat wal segment: %w", err)
	}
	if stat.Size() < offset {
		_ = file.Close()

		return &walSegmentOffsetMismatchError{
			segmentName: segmentName,
			size:        stat.Size(),
			offset:      offset,
		}
	}
	if stat.Size() > offset {
		if err := file.Truncate(offset); err != nil {
			_ = file.Close()

			return fmt.Errorf("failed to truncate wal segment: %w", err)
		}
	}

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		_ = file.Close()

		return fmt.Errorf("failed to seek wal segment: %w", err)
	}

	if err := writeAll(file, segmentData); err != nil {
		_ = file.Close()

		return fmt.Errorf("failed to write wal chunk: %w", err)
	}

	syncErr := file.Sync()
	closeErr := file.Close()
	if syncErr != nil {
		return fmt.Errorf("failed to sync wal segment: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("failed to close wal segment: %w", closeErr)
	}

	if offset == 0 {
		if err := syncDirectory(s.walDirectory); err != nil {
			return fmt.Errorf("failed to sync wal directory: %w", err)
		}
	}

	return nil
}

func (s *Slave) rewindWALCursor(err *walSegmentOffsetMismatchError) {
	if err == nil {
		return
	}

	s.lastSegmentName = err.segmentName
	s.lastSegmentOffset = err.size

	s.logger.Warn().
		Str("segment_name", err.segmentName).
		Int64("local_segment_size", err.size).
		Int64("requested_offset", err.offset).
		Uint64("last_applied_lsn", s.lastAppliedLSN).
		Msg("local WAL segment is behind replication cursor, rewinding WAL cursor")
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
//
//nolint:dupl // ok
func (s *Slave) sendToWALStream(ctx context.Context, chunk wal.Chunk) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error().Interface("panic", r).Msg("panic sending to walStream (channel closed)")
			err = fmt.Errorf("send to walStream: %v", r)
		}
	}()

	select {
	case s.walStream <- chunk:
		return nil
	default:
	}

	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()

	select {
	case s.walStream <- chunk:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closeCh:
		return errSlaveClosed
	case <-timer.C:
		return fmt.Errorf("timeout sending to walStream")
	}
}

func (s *Slave) applyDataToEngine(
	ctx context.Context,
	segmentData []byte,
	segmentName string,
	expectHeader bool,
) error {
	if len(segmentData) == 0 {
		s.logger.Warn().Str("segment_name", segmentName).Msg("received empty segment data, skipping")
		return nil
	}

	logs, err := s.walReader.ReadSegmentData(ctx, segmentData, expectHeader)
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
	applied := make(chan error, 1)
	chunk := wal.Chunk{
		Logs:    logsToApply,
		Applied: applied,
	}
	if err := s.sendToWALStream(ctx, chunk); err != nil {
		return fmt.Errorf("failed to send WAL data to stream: %w", err)
	}

	if err := s.waitForWALChunkApplied(ctx, applied); err != nil {
		return fmt.Errorf("wait for WAL apply: %w", err)
	}

	// Update last applied LSN after the engine has applied the chunk.
	s.lastAppliedLSN = lastLSN
	observability.SetReplicationLagLSN(0)

	return nil
}

func (s *Slave) waitForWALChunkApplied(ctx context.Context, applied <-chan error) error {
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
