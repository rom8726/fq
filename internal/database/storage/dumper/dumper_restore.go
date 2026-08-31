package dumper

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"os"

	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/database/storage/format"
)

func (d *Dumper) Restore(ctx context.Context) (database.Tx, error) {
	dumpPath := d.currentDumpFilePath()

	data, err := os.ReadFile(dumpPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}

		return 0, fmt.Errorf("failed to read dump file: %w", err)
	}

	frames, err := format.ParseHeader(data, format.MagicDump, dumpFormatVersion)
	if err != nil {
		return 0, fmt.Errorf("dump %s: %w", dumpPath, err)
	}

	var lastTx database.Tx
	batchCount := 0
	offset := 0

	for offset < len(frames) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		payload, rest, err := format.NextFrame(frames[offset:], dumpMaxFrameSize)
		if err != nil {
			return lastTx, fmt.Errorf(
				"dump %s: batch #%d at offset %d: %w",
				dumpPath,
				batchCount,
				format.HeaderSize+offset,
				err,
			)
		}

		var batch []database.DumpElem
		if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(&batch); err != nil {
			return lastTx, fmt.Errorf("dump %s: failed to decode batch #%d: %w", dumpPath, batchCount, err)
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

		offset = len(frames) - len(rest)
	}

	return lastTx, nil
}
