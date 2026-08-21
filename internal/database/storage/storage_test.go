package storage

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"fq/internal/database"
)

func TestLoadWALWithoutWALContinuesAfterDumpLastTx(t *testing.T) {
	engine := &txRecordingEngine{}
	logger := zerolog.Nop()
	strg, err := NewStorage(engine, nil, nil, nil, &logger, time.Hour, time.Hour, false)
	require.NoError(t, err)

	require.NoError(t, strg.LoadWAL(context.Background(), database.Tx(41)))

	_, err = strg.Incr(context.Background(), database.BatchKey{Key: "key", BatchSize: 60, BatchSizeStr: "60"})
	require.NoError(t, err)
	require.Equal(t, database.Tx(42), engine.lastTx)
}

type txRecordingEngine struct {
	lastTx database.Tx
}

func (e *txRecordingEngine) Incr(txCtx database.TxContext, _ database.BatchKey) database.ValueType {
	e.lastTx = txCtx.Tx

	return 1
}

func (e *txRecordingEngine) Get(database.BatchKey) (database.ValueType, bool) {
	return 0, false
}

func (e *txRecordingEngine) Del(database.TxContext, database.BatchKey) bool {
	return false
}

func (e *txRecordingEngine) MDel(database.TxContext, []database.BatchKey) []bool {
	return nil
}

func (e *txRecordingEngine) Clean(context.Context) {}

func (e *txRecordingEngine) Dump(context.Context, database.Tx) (<-chan database.DumpElem, <-chan error) {
	elems := make(chan database.DumpElem)
	errs := make(chan error, 1)
	close(elems)
	close(errs)

	return elems, errs
}

func (e *txRecordingEngine) RestoreDumpElem(context.Context, database.DumpElem) error {
	return nil
}
