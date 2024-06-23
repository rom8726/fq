package wal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

func (w *WAL) RemovePastSegments(ctx context.Context, lsn uint64) error {
	files, err := os.ReadDir(w.directory)
	if err != nil {
		return fmt.Errorf("failed to scan WAL directory: %w", err)
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filePath := filepath.Join(w.directory, file.Name())
		logs, err := w.fsReader.ReadSegment(ctx, filePath)
		if err != nil {
			return fmt.Errorf("failed to read segment %s: %w", filePath, err)
		}

		if len(logs) == 0 {
			continue
		}

		sort.Slice(logs, func(i, j int) bool {
			return logs[i].LSN < logs[j].LSN
		})

		if logs[len(logs)-1].LSN < lsn {
			w.logger.Debug().Msg(fmt.Sprintf("removing segment %s", filePath))

			if err := os.Remove(filePath); err != nil {
				return fmt.Errorf("failed to remove segment %s: %w", filePath, err)
			}
		}
	}

	return nil
}
