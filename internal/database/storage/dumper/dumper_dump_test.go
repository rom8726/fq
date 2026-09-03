package dumper

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/database/storage/format"
	"github.com/fq-db/fq/internal/database/storage/format/formattest"
)

func TestDumperWALCleanupUsesDumpTxWithoutProvider(t *testing.T) {
	wal := &recordingWAL{}
	d := New(emptyDumpEngine{}, wal, t.TempDir(), format.Compression{})
	defer d.Shutdown()

	require.NoError(t, d.Dump(context.Background(), database.Tx(100)))

	require.Eventually(t, func() bool {
		return wal.equalLSNs([]uint64{100})
	}, time.Second, 10*time.Millisecond)
}

func TestDumperCreatesMissingDumpDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing", "dump")
	d := New(emptyDumpEngine{}, nil, dir, format.Compression{})
	defer d.Shutdown()

	require.NoError(t, d.Dump(context.Background(), database.Tx(100)))

	_, err := os.Stat(filepath.Join(dir, currentDumpFileName))
	require.NoError(t, err)
}

func TestDumperWALCleanupIsCappedByReplicaAck(t *testing.T) {
	wal := &recordingWAL{}
	d := New(emptyDumpEngine{}, wal, t.TempDir(), format.Compression{})
	defer d.Shutdown()
	d.SetWALCleanupLSNProvider(staticCleanupLSNProvider{lsn: 40, ok: true})

	require.NoError(t, d.Dump(context.Background(), database.Tx(100)))

	require.Eventually(t, func() bool {
		return wal.equalLSNs([]uint64{40})
	}, time.Second, 10*time.Millisecond)
}

func TestDumperWALCleanupUsesDumpTxWhenReplicaAckIsAhead(t *testing.T) {
	wal := &recordingWAL{}
	d := New(emptyDumpEngine{}, wal, t.TempDir(), format.Compression{})
	defer d.Shutdown()
	d.SetWALCleanupLSNProvider(staticCleanupLSNProvider{lsn: 150, ok: true})

	require.NoError(t, d.Dump(context.Background(), database.Tx(100)))

	require.Eventually(t, func() bool {
		return wal.equalLSNs([]uint64{100})
	}, time.Second, 10*time.Millisecond)
}

func TestDumperWALCleanupKeepsWALWhenReplicaAckIsZero(t *testing.T) {
	wal := &recordingWAL{}
	d := New(emptyDumpEngine{}, wal, t.TempDir(), format.Compression{})
	defer d.Shutdown()
	d.SetWALCleanupLSNProvider(staticCleanupLSNProvider{lsn: 0, ok: true})

	require.NoError(t, d.Dump(context.Background(), database.Tx(100)))

	require.Eventually(t, func() bool {
		return wal.equalLSNs([]uint64{0})
	}, time.Second, 10*time.Millisecond)
}

func TestDumperDoesNotWaitForWALCleanup(t *testing.T) {
	wal := &blockingWAL{release: make(chan struct{})}
	d := New(emptyDumpEngine{}, wal, t.TempDir(), format.Compression{})

	require.NoError(t, d.Dump(context.Background(), database.Tx(100)))
	require.Eventually(t, func() bool {
		return wal.startedCleanup()
	}, time.Second, 10*time.Millisecond)

	close(wal.release)
	d.Shutdown()
	require.True(t, wal.equalLSNs([]uint64{100}))
}

func TestDumperShutdownCancelsRunningWALCleanup(t *testing.T) {
	wal := &blockingWAL{release: make(chan struct{})}
	d := New(emptyDumpEngine{}, wal, t.TempDir(), format.Compression{})

	require.NoError(t, d.Dump(context.Background(), database.Tx(100)))
	require.Eventually(t, func() bool {
		return wal.startedCleanup()
	}, time.Second, 10*time.Millisecond)

	done := make(chan struct{})
	go func() {
		d.Shutdown()
		close(done)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

type emptyDumpEngine struct{}

func (e emptyDumpEngine) Dump(context.Context, database.Tx) (<-chan database.DumpElem, <-chan error) {
	elems := make(chan database.DumpElem)
	errs := make(chan error, 1)
	close(elems)
	errs <- nil
	close(errs)

	return elems, errs
}

func (e emptyDumpEngine) RestoreDumpElem(context.Context, database.DumpElem) error {
	return nil
}

type recordingWAL struct {
	mu   sync.Mutex
	lsns []uint64
}

func (w *recordingWAL) RemovePastSegments(_ context.Context, lsn uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.lsns = append(w.lsns, lsn)

	return nil
}

func (w *recordingWAL) equalLSNs(expected []uint64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	return slices.Equal(w.lsns, expected)
}

type blockingWAL struct {
	recordingWAL
	release chan struct{}
	started bool
}

func (w *blockingWAL) RemovePastSegments(ctx context.Context, lsn uint64) error {
	w.mu.Lock()
	w.started = true
	w.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.release:
	}

	return w.recordingWAL.RemovePastSegments(ctx, lsn)
}

func (w *blockingWAL) startedCleanup() bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.started
}

type staticCleanupLSNProvider struct {
	lsn uint64
	ok  bool
}

func (p staticCleanupLSNProvider) WALCleanupLSN() (uint64, bool) {
	return p.lsn, p.ok
}

func TestDumpFileStartsWithFormatHeader(t *testing.T) {
	dir := t.TempDir()
	d := New(emptyDumpEngine{}, nil, dir, format.Compression{})
	defer d.Shutdown()

	require.NoError(t, d.Dump(context.Background(), database.Tx(100)))

	data, err := os.ReadFile(filepath.Join(dir, currentDumpFileName))
	require.NoError(t, err)

	rest, err := format.ParseHeader(data, format.MagicDump, dumpFormatVersionRaw)
	require.NoError(t, err)
	require.Empty(t, rest)
}

func TestRestoreReadsBackWrittenElements(t *testing.T) {
	dir := t.TempDir()
	engine := &recordingRestoreEngine{}
	d := New(engine, nil, dir, format.Compression{})
	defer d.Shutdown()

	require.NoError(t, d.Dump(context.Background(), database.Tx(1)))

	tx, err := d.Restore(context.Background())
	require.NoError(t, err)
	require.Equal(t, database.Tx(1), tx)
	require.Len(t, engine.restored, 1)
}

func TestRestoreRejectsChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	engine := &recordingRestoreEngine{}
	d := New(engine, nil, dir, format.Compression{})
	defer d.Shutdown()

	require.NoError(t, d.Dump(context.Background(), database.Tx(1)))

	dumpPath := filepath.Join(dir, currentDumpFileName)
	data, err := os.ReadFile(dumpPath)
	require.NoError(t, err)
	require.Greater(t, len(data), format.HeaderSize+format.FrameHeaderSize)
	require.NoError(t, os.WriteFile(dumpPath, formattest.CorruptPayload(t, data, format.HeaderSize), 0o600))

	_, err = d.Restore(context.Background())
	require.ErrorIs(t, err, format.ErrChecksumMismatch)
}

func TestRestoreRejectsForeignMagic(t *testing.T) {
	dir := t.TempDir()
	d := New(emptyDumpEngine{}, nil, dir, format.Compression{})
	defer d.Shutdown()

	require.NoError(t, d.Dump(context.Background(), database.Tx(1)))

	dumpPath := filepath.Join(dir, currentDumpFileName)
	data, err := os.ReadFile(dumpPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dumpPath, formattest.CorruptMagic(t, data), 0o600))

	_, err = d.Restore(context.Background())
	require.ErrorIs(t, err, format.ErrBadMagic)
}

func TestRestoreRejectsUnknownFormatVersion(t *testing.T) {
	dir := t.TempDir()
	d := New(emptyDumpEngine{}, nil, dir, format.Compression{})
	defer d.Shutdown()

	require.NoError(t, d.Dump(context.Background(), database.Tx(1)))

	dumpPath := filepath.Join(dir, currentDumpFileName)
	data, err := os.ReadFile(dumpPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dumpPath, formattest.SetVersion(t, data, dumpFormatVersionCompressed+1), 0o600))

	_, err = d.Restore(context.Background())
	require.ErrorIs(t, err, format.ErrUnsupportedVersion)
}

func TestRestoreRejectsEmptyDumpFile(t *testing.T) {
	dir := t.TempDir()
	d := New(emptyDumpEngine{}, nil, dir, format.Compression{})
	defer d.Shutdown()

	require.NoError(t, os.WriteFile(filepath.Join(dir, currentDumpFileName), nil, 0o600))

	_, err := d.Restore(context.Background())
	require.ErrorIs(t, err, format.ErrIncompleteFrame)
}

func TestRestoreReturnsZeroWhenDumpIsMissing(t *testing.T) {
	d := New(emptyDumpEngine{}, nil, t.TempDir(), format.Compression{})
	defer d.Shutdown()

	tx, err := d.Restore(context.Background())
	require.NoError(t, err)
	require.Zero(t, tx)
}

func TestGetNextDataRejectsChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	engine := &recordingRestoreEngine{}
	d := New(engine, nil, dir, format.Compression{})
	defer d.Shutdown()

	require.NoError(t, d.Dump(context.Background(), database.Tx(1)))

	dumpPath := filepath.Join(dir, currentDumpFileName)
	data, err := os.ReadFile(dumpPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dumpPath, formattest.CorruptPayload(t, data, format.HeaderSize), 0o600))

	_, ok, err := d.GetNextData("session-1")
	require.False(t, ok)
	require.ErrorIs(t, err, format.ErrChecksumMismatch)
}

type recordingRestoreEngine struct {
	restored []database.DumpElem
}

func (e *recordingRestoreEngine) Dump(context.Context, database.Tx) (<-chan database.DumpElem, <-chan error) {
	elems := make(chan database.DumpElem, 1)
	errs := make(chan error, 1)

	elems <- database.DumpElem{Tx: 1, Key: "key"}
	close(elems)
	errs <- nil
	close(errs)

	return elems, errs
}

func (e *recordingRestoreEngine) RestoreDumpElem(_ context.Context, elem database.DumpElem) error {
	e.restored = append(e.restored, elem)

	return nil
}

func TestCompressedDumpRoundTrip(t *testing.T) {
	dir := t.TempDir()
	compression := format.Compression{Codec: format.CodecZstd, MinFrameSize: 0}
	d := New(&recordingRestoreEngine{}, nil, dir, compression)
	defer d.Shutdown()

	require.NoError(t, d.Dump(context.Background(), database.Tx(10)))

	data, err := os.ReadFile(filepath.Join(dir, currentDumpFileName))
	require.NoError(t, err)

	_, version, err := format.ParseHeaderVersions(
		data,
		format.MagicDump,
		dumpFormatVersionRaw,
		dumpFormatVersionCompressed,
	)
	require.NoError(t, err)
	require.Equal(t, dumpFormatVersionCompressed, version)

	engine := &recordingRestoreEngine{}
	reader := New(engine, nil, dir, format.Compression{})
	defer reader.Shutdown()

	lastTx, err := reader.Restore(context.Background())
	require.NoError(t, err)
	require.Equal(t, database.Tx(1), lastTx)
	require.Len(t, engine.restored, 1)
}

func TestUncompressedDumpKeepsFormatVersionOne(t *testing.T) {
	dir := t.TempDir()
	d := New(&recordingRestoreEngine{}, nil, dir, format.Compression{})
	defer d.Shutdown()

	require.NoError(t, d.Dump(context.Background(), database.Tx(3)))

	data, err := os.ReadFile(filepath.Join(dir, currentDumpFileName))
	require.NoError(t, err)

	_, version, err := format.ParseHeaderVersions(
		data,
		format.MagicDump,
		dumpFormatVersionRaw,
		dumpFormatVersionCompressed,
	)
	require.NoError(t, err)
	require.Equal(t, dumpFormatVersionRaw, version)
}
