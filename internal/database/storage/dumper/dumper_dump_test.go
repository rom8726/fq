package dumper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"fq/internal/database"
)

func TestDumperWALCleanupUsesDumpTxWithoutProvider(t *testing.T) {
	wal := &recordingWAL{}
	d := New(emptyDumpEngine{}, wal, t.TempDir())
	defer d.Shutdown()

	require.NoError(t, d.Dump(context.Background(), database.Tx(100)))

	require.Equal(t, []uint64{100}, wal.lsns)
}

func TestDumperWALCleanupIsCappedByReplicaAck(t *testing.T) {
	wal := &recordingWAL{}
	d := New(emptyDumpEngine{}, wal, t.TempDir())
	defer d.Shutdown()
	d.SetWALCleanupLSNProvider(staticCleanupLSNProvider{lsn: 40, ok: true})

	require.NoError(t, d.Dump(context.Background(), database.Tx(100)))

	require.Equal(t, []uint64{40}, wal.lsns)
}

func TestDumperWALCleanupUsesDumpTxWhenReplicaAckIsAhead(t *testing.T) {
	wal := &recordingWAL{}
	d := New(emptyDumpEngine{}, wal, t.TempDir())
	defer d.Shutdown()
	d.SetWALCleanupLSNProvider(staticCleanupLSNProvider{lsn: 150, ok: true})

	require.NoError(t, d.Dump(context.Background(), database.Tx(100)))

	require.Equal(t, []uint64{100}, wal.lsns)
}

func TestDumperWALCleanupKeepsWALWhenReplicaAckIsZero(t *testing.T) {
	wal := &recordingWAL{}
	d := New(emptyDumpEngine{}, wal, t.TempDir())
	defer d.Shutdown()
	d.SetWALCleanupLSNProvider(staticCleanupLSNProvider{lsn: 0, ok: true})

	require.NoError(t, d.Dump(context.Background(), database.Tx(100)))

	require.Equal(t, []uint64{0}, wal.lsns)
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
	lsns []uint64
}

func (w *recordingWAL) RemovePastSegments(_ context.Context, lsn uint64) error {
	w.lsns = append(w.lsns, lsn)

	return nil
}

type staticCleanupLSNProvider struct {
	lsn uint64
	ok  bool
}

func (p staticCleanupLSNProvider) WALCleanupLSN() (uint64, bool) {
	return p.lsn, p.ok
}
