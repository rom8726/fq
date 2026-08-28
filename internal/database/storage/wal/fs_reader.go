package wal

import (
	"context"
	"fmt"
	"os"
	"sort"

	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
)

const batchMaxSize = 100 * 1024 * 1024
const batchSizeHeaderSize = 4

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

		data, err := os.ReadFile(filename)
		if err != nil {
			return nil, fmt.Errorf("failed to read WAL segment %s: %w", filename, err)
		}

		isLastSegment := i == len(filenames)-1
		segmentedLogs, err := r.readSegmentData(ctx, data, isLastSegment, filename)
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

func (r *FSReader) ReadSegment(ctx context.Context, filename string) ([]*LogData, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	return r.readSegmentData(ctx, data, false, filename)
}

func (r *FSReader) ReadSegmentData(ctx context.Context, data []byte) ([]*LogData, error) {
	return r.readSegmentData(ctx, data, false, "")
}

func (r *FSReader) readSegmentData(
	ctx context.Context,
	data []byte,
	allowTruncatedTail bool,
	segmentName string,
) ([]*LogData, error) {
	var logs []*LogData
	offset := 0

	for offset < len(data) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		remaining := len(data) - offset
		if remaining < batchSizeHeaderSize {
			if allowTruncatedTail {
				if err := r.truncateTail(segmentName, int64(offset), offset, batchSizeHeaderSize, remaining); err != nil {
					return nil, err
				}

				return logs, nil
			}

			return nil, fmt.Errorf(
				"truncated WAL batch header at offset %d: got %d bytes, need %d",
				offset,
				remaining,
				batchSizeHeaderSize,
			)
		}

		batchOffset := offset
		batchSize := int(bytesToUint32(data[offset : offset+batchSizeHeaderSize]))
		offset += batchSizeHeaderSize
		if batchSize > batchMaxSize {
			return nil, fmt.Errorf("max batch size in WAL segment exceeded: %d (max: %d)", batchSize, batchMaxSize)
		}

		remaining = len(data) - offset
		if batchSize > remaining {
			if allowTruncatedTail {
				if err := r.truncateTail(
					segmentName,
					int64(batchOffset),
					batchOffset,
					batchSize+batchSizeHeaderSize,
					remaining+batchSizeHeaderSize,
				); err != nil {
					return nil, err
				}

				return logs, nil
			}

			return nil, fmt.Errorf(
				"truncated WAL batch data at offset %d: declared %d bytes, got %d",
				offset,
				batchSize,
				remaining,
			)
		}

		batchData := data[offset : offset+batchSize]
		offset += batchSize

		var batch LogDataArray
		if err := proto.Unmarshal(batchData, &batch); err != nil {
			return nil, fmt.Errorf("failed to unmarshal WAL batch at offset %d: %w", batchOffset, err)
		}

		logs = append(logs, batch.Elems...)
	}

	return logs, nil
}

func (r *FSReader) truncateTail(segmentName string, validSize int64, offset, expected, actual int) error {
	if r.logger != nil {
		r.logger.Warn().
			Str("segment_name", segmentName).
			Int("offset", offset).
			Int("expected_bytes", expected).
			Int("actual_bytes", actual).
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
