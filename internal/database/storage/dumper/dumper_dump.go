package dumper

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"fq/internal/database"
)

func (d *Dumper) Dump(ctx context.Context, dumpTx database.Tx) error {
	// Lock write access during dump creation
	d.readDumpMu.Lock()
	defer d.readDumpMu.Unlock()

	// Invalidate all active sessions before creating new dump
	d.invalidateAllSessions()

	filename := fmt.Sprintf("dump_%d.dump", time.Now().UnixNano())
	filePath := filepath.Join(d.dir, filename)
	shouldRemove := true
	defer func() {
		if shouldRemove {
			_ = os.Remove(filePath)
		}
	}()

	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("opening dump file: %w", err)
	}

	defer func() { _ = f.Close() }()

	dumpBatch := make([]database.DumpElem, 0, dumpBatchSize)

	elemsC, errC := d.engine.Dump(ctx, dumpTx)
	for elem := range elemsC {
		dumpBatch = append(dumpBatch, elem)
		if len(dumpBatch) >= dumpBatchSize {
			err := d.writeBatch(f, dumpBatch)
			if err != nil {
				return fmt.Errorf("write batch: %w", err)
			}

			dumpBatch = dumpBatch[:0]
		}
	}

	if err := <-errC; err != nil {
		return fmt.Errorf("dump engine: %w", err)
	}

	if len(dumpBatch) > 0 {
		if err := d.writeBatch(f, dumpBatch); err != nil {
			return fmt.Errorf("write batch: %w", err)
		}
	}

	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync dump file: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close dump file: %w", err)
	}

	if err := os.Rename(filePath, d.currentDumpFilePath()); err != nil {
		return fmt.Errorf("rename dump file: %w", err)
	}

	// Increment dump version after successful rename
	d.dumpVersion++
	shouldRemove = false // File successfully renamed, don't remove

	if d.wal != nil {
		if err := d.wal.RemovePastSegments(ctx, uint64(dumpTx)); err != nil {
			return fmt.Errorf("remove past WAL segments: %w", err)
		}
	}

	return nil
}

func (d *Dumper) writeBatch(f *os.File, elems []database.DumpElem) error {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(elems); err != nil {
		return fmt.Errorf("encode dump elements: %w", err)
	}

	_, err := f.Write(buffer.Bytes())
	if err != nil {
		return fmt.Errorf("write dump elements: %w", err)
	}

	return nil
}
