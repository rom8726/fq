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
)

func TestDumperWALCleanupUsesDumpTxWithoutProvider(t *testing.T) {
	wal := &recordingWAL{}
	d := New(emptyDumpEngine{}, wal, t.TempDir())
	defer d.Shutdown()

	require.NoError(t, d.Dump(context.Background(), database.Tx(100)))

	require.Eventually(t, func() bool {
		return wal.equalLSNs([]uint64{100})
	}, time.Second, 10*time.Millisecond)
}

func TestDumperCreatesMissingDumpDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing", "dump")
	d := New(emptyDumpEngine{}, nil, dir)
	defer d.Shutdown()

	require.NoError(t, d.Dump(context.Background(), database.Tx(100)))

	_, err := os.Stat(filepath.Join(dir, currentDumpFileName))
	require.NoError(t, err)
}

func TestDumperWALCleanupIsCappedByReplicaAck(t *testing.T) {
	wal := &recordingWAL{}
	d := New(emptyDumpEngine{}, wal, t.TempDir())
	defer d.Shutdown()
	d.SetWALCleanupLSNProvider(staticCleanupLSNProvider{lsn: 40, ok: true})

	require.NoError(t, d.Dump(context.Background(), database.Tx(100)))

	require.Eventually(t, func() bool {
		return wal.equalLSNs([]uint64{40})
	}, time.Second, 10*time.Millisecond)
}

func TestDumperWALCleanupUsesDumpTxWhenReplicaAckIsAhead(t *testing.T) {
	wal := &recordingWAL{}
	d := New(emptyDumpEngine{}, wal, t.TempDir())
	defer d.Shutdown()
	d.SetWALCleanupLSNProvider(staticCleanupLSNProvider{lsn: 150, ok: true})

	require.NoError(t, d.Dump(context.Background(), database.Tx(100)))

	require.Eventually(t, func() bool {
		return wal.equalLSNs([]uint64{100})
	}, time.Second, 10*time.Millisecond)
}

func TestDumperWALCleanupKeepsWALWhenReplicaAckIsZero(t *testing.T) {
	wal := &recordingWAL{}
	d := New(emptyDumpEngine{}, wal, t.TempDir())
	defer d.Shutdown()
	d.SetWALCleanupLSNProvider(staticCleanupLSNProvider{lsn: 0, ok: true})

	require.NoError(t, d.Dump(context.Background(), database.Tx(100)))

	require.Eventually(t, func() bool {
		return wal.equalLSNs([]uint64{0})
	}, time.Second, 10*time.Millisecond)
}

func TestDumperDoesNotWaitForWALCleanup(t *testing.T) {
	wal := &blockingWAL{release: make(chan struct{})}
	d := New(emptyDumpEngine{}, wal, t.TempDir())

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
	d := New(emptyDumpEngine{}, wal, t.TempDir())

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
