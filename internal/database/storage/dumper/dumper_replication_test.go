package dumper

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"fq/internal/database"
	inMemory "fq/internal/database/storage/engine/in-memory"
)

func TestDumper_GetNextData(t *testing.T) {
	logger := zerolog.Nop()
	engine, err := inMemory.NewEngine(inMemory.HashTableBuilder, 1, &logger, nil, nil)
	require.NoError(t, err)

	now := time.Now().Unix()
	engine.Incr(
		database.TxContext{
			Tx:       0,
			DumpTx:   0,
			CurrTime: database.TxTime(now),
			FromWAL:  false,
		},
		database.BatchKey{
			BatchSize:    60,
			BatchSizeStr: "60",
			Key:          "key1",
		},
	)

	d := New(engine, nil, "/tmp")
	err = d.Dump(context.Background(), 1)
	require.NoError(t, err)

	sessionUUID := "session1"
	batch, ok, err := d.GetNextData(sessionUUID)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, 1, len(batch))

	batch, ok, err = d.GetNextData(sessionUUID)
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, 0, len(batch))
}
