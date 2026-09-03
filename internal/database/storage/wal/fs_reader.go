package wal

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"

	"github.com/fq-db/fq/internal/database/storage/format"
)

type FSReader struct {
	directory string
	logger    *zerolog.Logger
}

func NewFSReader(directory string, logger *zerolog.Logger) *FSReader {
	return &FSReader{
		directory: directory,
		logger:    logger,
	}
}

func (r *FSReader) ReadLogs(ctx context.Context) ([]*LogData, error) {
	return r.ReadLogsAfter(ctx, 0)
}

func (r *FSReader) ReadLogsAfter(ctx context.Context, lsn uint64) ([]*LogData, error) {
	filenames, err := walSegmentPaths(r.directory)
	if err != nil {
		return nil, err
	}
	sort.Strings(filenames)

	var logs []*LogData
	for i, filename := range filenames {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		if canSkip, err := r.segmentCanBeSkipped(ctx, filename, lsn); err != nil {
			return nil, err
		} else if canSkip {
			continue
		}

		data, err := os.ReadFile(filename)
		if err != nil {
			return nil, fmt.Errorf("failed to read WAL segment %s: %w", filename, err)
		}

		if len(data) == 0 {
			if r.logger != nil {
				r.logger.Warn().Str("segment_path", filename).Msg("skipping empty WAL segment")
			}

			continue
		}

		isLastSegment := i == len(filenames)-1
		segmentedLogs, err := r.readSegmentData(ctx, data, true, isLastSegment, filename, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to recover WAL segment %s: %w", filename, err)
		}

		logs = append(logs, segmentedLogs...)
	}

	sort.Slice(logs, func(i, j int) bool {
		return logs[i].LSN < logs[j].LSN
	})

	return logs, nil
}

func (r *FSReader) segmentCanBeSkipped(ctx context.Context, filename string, lsn uint64) (bool, error) {
	if lsn == 0 {
		return false, nil
	}

	meta, err := readSegmentMetadata(filename)
	if err == nil {
		return meta.MaxLSN <= lsn, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	if r.logger != nil {
		r.logger.Warn().
			Err(err).
			Str("segment_path", filename).
			Msg("failed to read WAL segment metadata, falling back to segment scan")
	}

	logs, err := r.ReadSegment(ctx, filename)
	if err != nil {
		return false, fmt.Errorf("failed to read segment metadata fallback %s: %w", filename, err)
	}

	var maxLSN uint64
	for _, log := range logs {
		if log.LSN > maxLSN {
			maxLSN = log.LSN
		}
	}

	return maxLSN <= lsn, nil
}

func (r *FSReader) ReadSegment(ctx context.Context, filename string) ([]*LogData, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	if len(data) == 0 {
		return nil, nil
	}

	return r.readSegmentData(ctx, data, true, false, filename, 0)
}

func (r *FSReader) ReadSegmentData(
	ctx context.Context,
	data []byte,
	expectHeader bool,
	version uint16,
) ([]*LogData, error) {
	return r.readSegmentData(ctx, data, expectHeader, false, "", version)
}

func (r *FSReader) readSegmentData(
	ctx context.Context,
	data []byte,
	expectHeader bool,
	allowTruncatedTail bool,
	segmentName string,
	version uint16,
) ([]*LogData, error) {
	frames := data
	baseOffset := 0

	if version == 0 {
		version = segmentFormatVersionRaw
	}

	if expectHeader {
		rest, parsedVersion, err := format.ParseHeaderVersions(
			data,
			format.MagicWAL,
			segmentFormatVersionRaw,
			segmentFormatVersionCompressed,
		)
		if err != nil {
			if errors.Is(err, format.ErrIncompleteFrame) && allowTruncatedTail {
				if truncateErr := r.truncateTail(segmentName, 0, err); truncateErr != nil {
					return nil, truncateErr
				}

				return nil, nil
			}

			return nil, fmt.Errorf("WAL segment header: %w", err)
		}

		frames = rest
		version = parsedVersion
		baseOffset = format.HeaderSize
	}

	var logs []*LogData
	offset := 0

	for offset < len(frames) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		payload, rest, err := format.NextFrame(frames[offset:], MaxBatchSize)
		if err != nil {
			if errors.Is(err, format.ErrIncompleteFrame) && allowTruncatedTail {
				if truncateErr := r.truncateTail(segmentName, int64(baseOffset+offset), err); truncateErr != nil {
					return nil, truncateErr
				}

				return logs, nil
			}

			return nil, fmt.Errorf("WAL segment at offset %d: %w", baseOffset+offset, err)
		}

		decoded, err := format.DecodePayload(nil, payload, version, MaxBatchSize)
		if err != nil {
			return nil, fmt.Errorf("WAL segment payload at offset %d: %w", baseOffset+offset, err)
		}

		var batch LogDataArray
		if err := proto.Unmarshal(decoded, &batch); err != nil {
			return nil, fmt.Errorf("failed to unmarshal WAL batch at offset %d: %w", baseOffset+offset, err)
		}

		logs = append(logs, batch.Elems...)
		offset = len(frames) - len(rest)
	}

	return logs, nil
}

func (r *FSReader) truncateTail(segmentName string, validSize int64, reason error) error {
	if r.logger != nil {
		r.logger.Warn().
			Err(reason).
			Str("segment_name", segmentName).
			Int64("valid_size", validSize).
			Msg("truncating incomplete WAL tail during recovery")
	}

	if segmentName == "" {
		return nil
	}

	if err := os.Truncate(segmentName, validSize); err != nil {
		return fmt.Errorf("failed to truncate incomplete WAL tail: %w", err)
	}

	segment, err := os.OpenFile(segmentName, os.O_RDONLY, 0)
	if err != nil {
		return fmt.Errorf("failed to open truncated WAL segment for sync: %w", err)
	}

	syncErr := segment.Sync()
	closeErr := segment.Close()
	if syncErr != nil {
		return fmt.Errorf("failed to sync truncated WAL segment: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("failed to close truncated WAL segment: %w", closeErr)
	}

	return nil
}
