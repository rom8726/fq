package dumper

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"

	"fq/internal/database"
)

func (d *Dumper) Restore(ctx context.Context) (database.Tx, error) {
	dumpPath := d.currentDumpFilePath()

	fileInfo, err := os.Stat(dumpPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}

		return 0, fmt.Errorf("failed to stat current dump file: %w", err)
	}

	// Check that file is not empty
	if fileInfo.Size() == 0 {
		return 0, nil
	}

	data, err := os.ReadFile(dumpPath)
	if err != nil {
		return 0, fmt.Errorf("failed to read dump file: %w", err)
	}

	// Check that data is not empty after reading
	if len(data) == 0 {
		return 0, nil
	}

	var lastTx database.Tx
	batchCount := 0

	buffer := bytes.NewBuffer(data)
	for buffer.Len() > 0 {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			var batch []database.DumpElem
			decoder := gob.NewDecoder(buffer)

			// Save position before decoding for potential skipping of corrupted batches
			startPos := buffer.Len()

			if err := decoder.Decode(&batch); err != nil {
				// If it's not end of file and there's data, try to skip corrupted batch
				if buffer.Len() > 0 && !errors.Is(err, io.EOF) {
					// Try to find start of next batch, skipping corrupted data
					// This is a simple heuristic: if decoding failed, skip to next possible batch
					// In reality, gob format doesn't allow easy skipping of corrupted data,
					// so return error with batch information
					return lastTx, fmt.Errorf("failed to decode dump batch #%d at position %d: %w", batchCount, startPos, err)
				}

				// If it's EOF and we've processed at least one batch, it's normal
				if batchCount > 0 {
					return lastTx, nil
				}

				return 0, fmt.Errorf("failed to decode dump data: %w", err)
			}

			batchCount++

			for _, elem := range batch {
				if err := d.engine.RestoreDumpElem(ctx, elem); err != nil {
					return lastTx, fmt.Errorf("failed to restore dump elem (batch #%d, tx=%d): %w", batchCount, elem.Tx, err)
				}

				if elem.Tx > lastTx {
					lastTx = elem.Tx
				}
			}
		}
	}

	return lastTx, nil
}
