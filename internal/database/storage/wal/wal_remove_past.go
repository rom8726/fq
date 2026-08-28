package wal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

func (w *WAL) RemovePastSegments(ctx context.Context, lsn uint64) error {
	filenames, err := walSegmentNames(w.directory)
	if err != nil {
		return err
	}

	sort.Strings(filenames)
	if len(filenames) == 0 {
		return nil
	}

	lastSegmentName := filenames[len(filenames)-1]
	for _, filename := range filenames {
		if filename == lastSegmentName {
			continue
		}

		filePath := filepath.Join(w.directory, filename)
		maxLSN, err := w.segmentMaxLSN(ctx, filePath)
		if err != nil {
			return err
		}

		if maxLSN < lsn {
			w.logger.Debug().Msg(fmt.Sprintf("removing segment %s", filePath))

			if err := removeSegmentAndMetadata(filePath); err != nil {
				return fmt.Errorf("failed to remove segment %s: %w", filePath, err)
			}
		}
	}

	return nil
}

func (w *WAL) segmentMaxLSN(ctx context.Context, filePath string) (uint64, error) {
	meta, err := readSegmentMetadata(filePath)
	if err == nil {
		return meta.MaxLSN, nil
	}
	if !os.IsNotExist(err) && w.logger != nil {
		w.logger.Warn().
			Err(err).
			Str("segment_path", filePath).
			Msg("failed to read WAL segment metadata, falling back to segment scan")
	}

	logs, err := w.fsReader.ReadSegment(ctx, filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to read segment %s: %w", filePath, err)
	}

	var maxLSN uint64
	for _, log := range logs {
		if log.LSN > maxLSN {
			maxLSN = log.LSN
		}
	}

	return maxLSN, nil
}
