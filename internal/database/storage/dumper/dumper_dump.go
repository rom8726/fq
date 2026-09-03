package dumper

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/database/storage/format"
	"github.com/fq-db/fq/internal/observability"
)

func (d *Dumper) Dump(ctx context.Context, dumpTx database.Tx) error {
	// Lock write access during dump creation
	d.readDumpMu.Lock()
	defer d.readDumpMu.Unlock()

	// Invalidate all active sessions before creating new dump
	d.invalidateAllSessions()

	if err := os.MkdirAll(d.dir, 0o750); err != nil {
		return fmt.Errorf("create dump directory: %w", err)
	}

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

	if err := format.WriteHeader(f, format.MagicDump, d.formatVersion()); err != nil {
		return fmt.Errorf("write dump header: %w", err)
	}

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

	if err := d.syncDumpDirectory(); err != nil {
		return err
	}

	// Increment dump version after successful rename
	d.dumpVersion++
	shouldRemove = false // File successfully renamed, don't remove

	if d.wal != nil {
		cleanupLSN := d.walCleanupLSN(uint64(dumpTx))
		d.scheduleWALCleanup(cleanupLSN)
	}

	return nil
}

func (d *Dumper) Truncate(ctx context.Context) error {
	d.readDumpMu.Lock()
	defer d.readDumpMu.Unlock()

	d.invalidateAllSessions()

	if err := os.MkdirAll(d.dir, 0o750); err != nil {
		return fmt.Errorf("create dump directory: %w", err)
	}

	files, err := os.ReadDir(d.dir)
	if err != nil {
		return fmt.Errorf("scan dump directory: %w", err)
	}

	for _, file := range files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		name := file.Name()
		if file.IsDir() || (name != currentDumpFileName && filepath.Ext(name) != ".dump") {
			continue
		}

		if err := os.Remove(filepath.Join(d.dir, name)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove dump file %s: %w", name, err)
		}
	}

	if err := d.syncDumpDirectory(); err != nil {
		return err
	}

	d.dumpVersion++

	return nil
}

func (d *Dumper) syncDumpDirectory() error {
	dir, err := os.Open(d.dir)
	if err != nil {
		return fmt.Errorf("open dump directory for sync: %w", err)
	}

	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return fmt.Errorf("sync dump directory: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close dump directory after sync: %w", closeErr)
	}

	return nil
}

func (d *Dumper) walCleanupLSN(dumpLSN uint64) uint64 {
	if d.walCleanupLSNProvider == nil {
		return dumpLSN
	}

	replicaLSN, ok := d.walCleanupLSNProvider.WALCleanupLSN()
	if !ok || replicaLSN > dumpLSN {
		return dumpLSN
	}

	return replicaLSN
}

func (d *Dumper) writeBatch(f *os.File, elems []database.DumpElem) error {
	var buffer bytes.Buffer

	encoder := gob.NewEncoder(&buffer)
	if err := encoder.Encode(elems); err != nil {
		return fmt.Errorf("encode dump elements: %w", err)
	}

	payload := buffer.Bytes()
	if d.formatVersion() == dumpFormatVersionCompressed {
		raw := payload
		startedAt := time.Now()
		payload = format.EncodePayload(nil, raw, d.compression)
		observability.ObserveCompressionDuration("dump", "compress", time.Since(startedAt))
		observability.ObserveCompression("dump", len(raw), len(payload))
	}

	if err := format.CheckPayloadSize(payload, dumpMaxFrameSize); err != nil {
		return fmt.Errorf("dump batch: %w", err)
	}

	if _, err := f.Write(format.AppendFrame(nil, payload)); err != nil {
		return fmt.Errorf("write dump elements: %w", err)
	}

	return nil
}
