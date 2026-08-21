package inmemory

import (
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"fq/internal/database"
)

func TestEngineAcknowledgesDumpChunkAfterApply(t *testing.T) {
	dumpStream := make(chan database.DumpChunk, 1)
	logger := zerolog.Nop()
	engine, err := NewEngine(HashTableBuilder, 1, &logger, nil, dumpStream)
	require.NoError(t, err)

	applied := make(chan error, 1)
	key := database.BatchKey{
		BatchSize:    60,
		BatchSizeStr: "60",
		Key:          "key",
	}
	dumpStream <- database.DumpChunk{
		Elems: []database.DumpElem{
			{
				Key:       key.Key,
				BatchSize: key.BatchSize,
				Value:     42,
				TxAt:      database.TxTime(time.Now().Unix()),
				Tx:        7,
			},
		},
		Applied: applied,
	}

	require.NoError(t, requireDumpAck(t, applied))
	value, found := engine.Get(key)
	require.True(t, found)
	require.Equal(t, database.ValueType(42), value)
}

func requireDumpAck(t *testing.T, applied <-chan error) error {
	t.Helper()

	select {
	case err := <-applied:
		return err
	case <-time.After(time.Second):
		t.Fatal("dump chunk was not acknowledged")
	}

	return nil
}
