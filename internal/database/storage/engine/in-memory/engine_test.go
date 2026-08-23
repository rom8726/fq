package inmemory

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database"
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

func TestEngineRLimitFixedWindowDoesNotExceedLimitConcurrently(t *testing.T) {
	logger := zerolog.Nop()
	engine, err := NewEngine(HashTableBuilder, 1, &logger, nil, nil)
	require.NoError(t, err)

	key := database.BatchKey{
		BatchSize:    60,
		BatchSizeStr: "60",
		Key:          "limited",
	}
	now := database.TxTime(time.Now().Unix())

	const limit = database.ValueType(10)
	const workers = 100

	var allowedCount atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			result, err := engine.RLimitFixedWindow(
				database.TxContext{Tx: database.Tx(i + 1), CurrTime: now},
				key,
				limit,
				nil,
			)
			require.NoError(t, err)
			if result.Allowed {
				allowedCount.Add(1)
			}
		}(i)
	}
	wg.Wait()

	require.Equal(t, int32(limit), allowedCount.Load())
	value, found := engine.Get(key)
	require.True(t, found)
	require.Equal(t, limit, value)
}

func TestEngineRLimitSlidingWindowDoesNotExceedLimitConcurrently(t *testing.T) {
	logger := zerolog.Nop()
	engine, err := NewEngine(HashTableBuilder, 1, &logger, nil, nil)
	require.NoError(t, err)

	key := database.BatchKey{
		BatchSize:    60,
		BatchSizeStr: "60",
		Key:          "limited",
	}
	now := database.TxTime(time.Now().Unix())

	const limit = database.ValueType(10)
	const workers = 100

	var allowedCount atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			result, err := engine.RLimitSlidingWindow(
				database.TxContext{Tx: database.Tx(i + 1), CurrTime: now},
				key,
				limit,
				nil,
			)
			require.NoError(t, err)
			if result.Allowed {
				allowedCount.Add(1)
			}
		}(i)
	}
	wg.Wait()

	require.Equal(t, int32(limit), allowedCount.Load())
}

func TestEngineRLimitTokenBucketDoesNotExceedCapacityConcurrently(t *testing.T) {
	logger := zerolog.Nop()
	engine, err := NewEngine(HashTableBuilder, 1, &logger, nil, nil)
	require.NoError(t, err)

	key := database.BatchKey{
		BatchSize:    60,
		BatchSizeStr: "60",
		Key:          "limited",
	}
	now := database.TxTime(time.Now().Unix())

	const capacity = database.ValueType(10)
	const refillAmount = database.ValueType(1)
	const workers = 100

	var allowedCount atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			result, err := engine.RLimitTokenBucket(
				database.TxContext{Tx: database.Tx(i + 1), CurrTime: now},
				key,
				capacity,
				refillAmount,
				nil,
			)
			require.NoError(t, err)
			if result.Allowed {
				allowedCount.Add(1)
			}
		}(i)
	}
	wg.Wait()

	require.Equal(t, int32(capacity), allowedCount.Load())
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
