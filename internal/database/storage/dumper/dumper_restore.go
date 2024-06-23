package dumper

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"os"

	"fq/internal/database"
)

func (d *Dumper) Restore(ctx context.Context) (database.Tx, error) {
	if _, err := os.Stat(d.currentDumpFilePath()); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}

		return 0, fmt.Errorf("failed to open current dump file: %w", err)
	}

	data, err := os.ReadFile(d.currentDumpFilePath())
	if err != nil {
		return 0, err
	}

	var lastTx database.Tx

	buffer := bytes.NewBuffer(data)
	for buffer.Len() > 0 {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			var batch []database.DumpElem
			decoder := gob.NewDecoder(buffer)
			if err := decoder.Decode(&batch); err != nil {
				return 0, fmt.Errorf("failed to decode dump data: %w", err)
			}

			for _, elem := range batch {
				if err := d.engine.RestoreDumpElem(ctx, elem); err != nil {
					return 0, fmt.Errorf("failed to restore dump elem: %w", err)
				}

				if elem.Tx > lastTx {
					lastTx = elem.Tx
				}
			}
		}
	}

	return lastTx, nil
}
