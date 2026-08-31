package inmemory

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/database/compute"
	"github.com/fq-db/fq/internal/database/storage/wal"
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

func TestEngineAppliesWALChunkConcurrentlyBeforeAck(t *testing.T) {
	walStream := make(chan wal.Chunk, 1)
	logger := zerolog.Nop()
	engine, err := NewEngineWithWALApplyWorkers(HashTableBuilder, 4, &logger, walStream, nil, 4)
	require.NoError(t, err)
	defer close(walStream)

	keyA, keyB := requireKeysFromDifferentPartitions(t, engine)
	now := strconv.FormatInt(time.Now().Unix(), 16)
	applied := make(chan error, 1)

	walStream <- wal.Chunk{
		Logs: []*wal.LogData{
			{
				LSN:       1,
				CommandId: uint32(compute.IncrCommandID),
				Arguments: []string{keyA.Key, keyA.BatchSizeStr, now},
			},
			{
				LSN:       2,
				CommandId: uint32(compute.IncrCommandID),
				Arguments: []string{keyB.Key, keyB.BatchSizeStr, now},
			},
			{
				LSN:       3,
				CommandId: uint32(compute.MDelCommandID),
				Arguments: []string{now, keyA.Key, keyA.BatchSizeStr, keyB.Key, keyB.BatchSizeStr},
			},
			{
				LSN:       4,
				CommandId: uint32(compute.IncrCommandID),
				Arguments: []string{keyB.Key, keyB.BatchSizeStr, now},
			},
			{
				LSN:       5,
				CommandId: uint32(compute.DelCommandID),
				Arguments: []string{keyB.Key, keyB.BatchSizeStr, now},
			},
			{
				LSN:       6,
				CommandId: uint32(compute.IncrCommandID),
				Arguments: []string{keyB.Key, keyB.BatchSizeStr, now},
			},
		},
		Applied: applied,
	}

	require.NoError(t, requireAck(t, applied))

	_, found := engine.Get(keyA)
	require.False(t, found)
	value, found := engine.Get(keyB)
	require.True(t, found)
	require.Equal(t, database.ValueType(1), value)
}

func TestEngineAppliesTruncateWALChunkBeforeAck(t *testing.T) {
	walStream := make(chan wal.Chunk, 1)
	logger := zerolog.Nop()
	engine, err := NewEngine(HashTableBuilder, 1, &logger, walStream, nil)
	require.NoError(t, err)
	defer close(walStream)

	key := database.BatchKey{
		BatchSize:    60,
		BatchSizeStr: "60",
		Key:          "key",
	}
	now := strconv.FormatInt(time.Now().Unix(), 16)
	applied := make(chan error, 1)

	walStream <- wal.Chunk{
		Logs: []*wal.LogData{
			{
				LSN:       1,
				CommandId: uint32(compute.IncrCommandID),
				Arguments: []string{key.Key, key.BatchSizeStr, now},
			},
			{
				LSN:       2,
				CommandId: uint32(compute.TruncateCommandID),
			},
		},
		Applied: applied,
	}

	require.NoError(t, requireAck(t, applied))
	_, found := engine.Get(key)
	require.False(t, found)
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

func TestEngineScanReturnsKeysInChunks(t *testing.T) {
	logger := zerolog.Nop()
	engine, err := NewEngineWithKeyIndex(IndexedHashTableBuilder, 1, &logger, nil, nil, true)
	require.NoError(t, err)

	now := database.TxTime(time.Now().Unix())
	engine.Incr(database.TxContext{Tx: 1, CurrTime: now}, database.BatchKey{Key: "alpha", BatchSize: 60, BatchSizeStr: "60"})
	engine.Incr(database.TxContext{Tx: 2, CurrTime: now}, database.BatchKey{Key: "bravo", BatchSize: 60, BatchSizeStr: "60"})
	engine.Incr(database.TxContext{Tx: 3, CurrTime: now}, database.BatchKey{Key: "charlie", BatchSize: 300, BatchSizeStr: "300"})

	first, err := engine.Scan("", "0", 2)
	require.NoError(t, err)
	require.Equal(t, []database.BatchKey{
		{Key: "alpha", BatchSize: 60, BatchSizeStr: "60"},
		{Key: "bravo", BatchSize: 60, BatchSizeStr: "60"},
	}, first.Keys)
	require.NotEqual(t, "0", first.NextCursor)

	second, err := engine.Scan("", first.NextCursor, 2)
	require.NoError(t, err)
	require.Equal(t, []database.BatchKey{
		{Key: "charlie", BatchSize: 300, BatchSizeStr: "300"},
	}, second.Keys)
	require.Equal(t, "0", second.NextCursor)
}

func TestEnginePScanReturnsOnlyMatchingPrefix(t *testing.T) {
	logger := zerolog.Nop()
	engine, err := NewEngineWithKeyIndex(IndexedHashTableBuilder, 1, &logger, nil, nil, true)
	require.NoError(t, err)

	now := database.TxTime(time.Now().Unix())
	engine.Incr(database.TxContext{Tx: 1, CurrTime: now}, database.BatchKey{Key: "tenant-a", BatchSize: 60, BatchSizeStr: "60"})
	engine.Incr(database.TxContext{Tx: 2, CurrTime: now}, database.BatchKey{Key: "tenant-b", BatchSize: 60, BatchSizeStr: "60"})
	engine.Incr(database.TxContext{Tx: 3, CurrTime: now}, database.BatchKey{Key: "other", BatchSize: 60, BatchSizeStr: "60"})

	result, err := engine.Scan("tenant-", "0", 10)
	require.NoError(t, err)
	require.Equal(t, []database.BatchKey{
		{Key: "tenant-a", BatchSize: 60, BatchSizeStr: "60"},
		{Key: "tenant-b", BatchSize: 60, BatchSizeStr: "60"},
	}, result.Keys)
	require.Equal(t, "0", result.NextCursor)
}

func TestEngineScanSkipsStaleIndexKeys(t *testing.T) {
	logger := zerolog.Nop()
	engine, err := NewEngineWithKeyIndex(IndexedHashTableBuilder, 1, &logger, nil, nil, true)
	require.NoError(t, err)

	now := database.TxTime(time.Now().Unix())
	stale := database.BatchKey{Key: "stale", BatchSize: 60, BatchSizeStr: "60"}
	live := database.BatchKey{Key: "tenant-live", BatchSize: 60, BatchSizeStr: "60"}
	engine.Incr(database.TxContext{Tx: 1, CurrTime: now}, stale)
	engine.Incr(database.TxContext{Tx: 2, CurrTime: now}, live)

	partition := engine.partitions[0].(*HashTable)
	partition.mu.Lock()
	delete(partition.m, hashTableKey{key: stale.Key, batchSize: stale.BatchSize})
	partition.mu.Unlock()

	result, err := engine.Scan("", "0", 10)
	require.NoError(t, err)
	require.Equal(t, []database.BatchKey{live}, result.Keys)
}

func TestEngineScanReturnsErrorWhenIndexIsDisabled(t *testing.T) {
	logger := zerolog.Nop()
	engine, err := NewEngine(HashTableBuilder, 1, &logger, nil, nil)
	require.NoError(t, err)

	_, err = engine.Scan("", "0", 10)
	require.ErrorIs(t, err, database.ErrScanIndexDisabled)
}

func requireDumpAck(t *testing.T, applied <-chan error) error {
	return requireAck(t, applied)
}

func requireAck(t *testing.T, applied <-chan error) error {
	t.Helper()

	select {
	case err := <-applied:
		return err
	case <-time.After(time.Second):
		t.Fatal("dump chunk was not acknowledged")
	}

	return nil
}

func requireKeysFromDifferentPartitions(t *testing.T, engine *Engine) (database.BatchKey, database.BatchKey) {
	t.Helper()

	first := database.BatchKey{
		BatchSize:    60,
		BatchSizeStr: "60",
		Key:          "key-0",
	}
	firstPartition := engine.partitionIdx(first.Key)

	for i := 1; i < 100; i++ {
		key := database.BatchKey{
			BatchSize:    60,
			BatchSizeStr: "60",
			Key:          "key-" + strconv.Itoa(i),
		}
		if engine.partitionIdx(key.Key) != firstPartition {
			return first, key
		}
	}

	t.Fatal("could not find keys from different partitions")
	return database.BatchKey{}, database.BatchKey{}
}
