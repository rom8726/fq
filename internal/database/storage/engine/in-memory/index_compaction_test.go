package inmemory

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database"
)

func addIndexedCounter(table *HashTable, key hashTableKey, lastTxAt database.TxTime) {
	elem := NewFqElem(key.batchSize)
	elem.lastTxAt = lastTxAt
	elem.value = 1

	table.mu.Lock()
	table.m[key] = elem
	table.indexKeyLocked(key)
	table.mu.Unlock()
}

func expiredCounterKeys(table *HashTable, count int, now database.TxTime) []hashTableKey {
	keys := make([]hashTableKey, 0, count)
	for i := 0; i < count; i++ {
		key := hashTableKey{key: fmt.Sprintf("expired-%06d", i), batchSize: 1}
		addIndexedCounter(table, key, now-expireDelta-2)
		keys = append(keys, key)
	}

	return keys
}

func TestHashTableCleanKeepsStaleIndexEntries(t *testing.T) {
	table := NewIndexedHashTable()
	now := database.TxTime(time.Now().Unix())

	key := hashTableKey{key: "expired", batchSize: 1}
	addIndexedCounter(table, key, now-expireDelta-2)

	table.Clean(context.Background())

	require.Empty(t, table.m)
	require.Equal(t, 1, table.index.Len())
	require.True(t, table.index.Has(key))
	require.Equal(t, uint64(1), table.indexStaleCount)
}

func TestHashTableScanSkipsStaleIndexEntries(t *testing.T) {
	table := NewIndexedHashTable()
	now := database.TxTime(time.Now().Unix())

	staleKey := hashTableKey{key: "aaa-expired", batchSize: 1}
	liveKey := hashTableKey{key: "bbb-live", batchSize: 1}
	addIndexedCounter(table, staleKey, now-expireDelta-2)
	addIndexedCounter(table, liveKey, now)

	table.Clean(context.Background())

	keys := table.Scan("", hashTableKey{}, 10)

	require.Len(t, keys, 1)
	require.Equal(t, liveKey.key, keys[0].Key)
	require.Equal(t, 2, table.index.Len())
	require.Equal(t, uint64(1), table.indexStaleCount)
}

func TestHashTableCompactIndexSkipsBelowThreshold(t *testing.T) {
	table := NewIndexedHashTable()
	now := database.TxTime(time.Now().Unix())

	expiredCounterKeys(table, indexCompactSmallStaleThreshold-1, now)
	table.Clean(context.Background())

	done := table.CompactIndex(context.Background(), 1024, time.Second)

	require.True(t, done)
	require.Equal(t, indexCompactSmallStaleThreshold-1, table.index.Len())
	require.Equal(t, uint64(indexCompactSmallStaleThreshold-1), table.indexStaleCount)
}

func TestHashTableCompactIndexRemovesStaleEntries(t *testing.T) {
	table := NewIndexedHashTable()
	now := database.TxTime(time.Now().Unix())

	total := indexCompactSmallStaleThreshold
	expiredCounterKeys(table, total, now)
	liveKey := hashTableKey{key: "live", batchSize: 1}
	addIndexedCounter(table, liveKey, now)

	table.Clean(context.Background())
	require.Equal(t, uint64(total), table.indexStaleCount)
	require.Equal(t, total+1, table.index.Len())

	done := table.CompactIndex(context.Background(), total*2, time.Second)

	require.True(t, done)
	require.Equal(t, 1, table.index.Len())
	require.True(t, table.index.Has(liveKey))
	require.Zero(t, table.indexStaleCount)
	require.False(t, table.indexCompactActive)
	require.Equal(t, hashTableKey{}, table.indexCompactAfter)
}

func TestHashTableCompactIndexRespectsMaxDeletes(t *testing.T) {
	table := NewIndexedHashTable()
	now := database.TxTime(time.Now().Unix())

	total := indexCompactSmallStaleThreshold
	keys := expiredCounterKeys(table, total, now)
	table.Clean(context.Background())

	done := table.CompactIndex(context.Background(), 10, time.Second)

	require.False(t, done)
	require.Equal(t, total-10, table.index.Len())
	require.Equal(t, uint64(total-10), table.indexStaleCount)
	require.True(t, table.indexCompactActive)
	require.Equal(t, keys[9], table.indexCompactAfter)

	for _, key := range keys[:10] {
		require.False(t, table.index.Has(key))
	}
	require.True(t, table.index.Has(keys[10]))
}

func TestHashTableCompactIndexContinuesFromCursor(t *testing.T) {
	table := NewIndexedHashTable()
	now := database.TxTime(time.Now().Unix())

	total := indexCompactSmallStaleThreshold
	keys := expiredCounterKeys(table, total, now)
	table.Clean(context.Background())

	require.False(t, table.CompactIndex(context.Background(), 10, time.Second))
	require.False(t, table.CompactIndex(context.Background(), 10, time.Second))

	require.Equal(t, total-20, table.index.Len())
	require.Equal(t, uint64(total-20), table.indexStaleCount)
	require.Equal(t, keys[19], table.indexCompactAfter)

	for _, key := range keys[:20] {
		require.False(t, table.index.Has(key))
	}
	require.True(t, table.index.Has(keys[20]))
}

func TestHashTableFlushDBResetsCompactionState(t *testing.T) {
	table := NewIndexedHashTable()
	now := database.TxTime(time.Now().Unix())

	expiredCounterKeys(table, indexCompactSmallStaleThreshold, now)
	table.Clean(context.Background())
	require.False(t, table.CompactIndex(context.Background(), 10, time.Second))
	require.True(t, table.indexCompactActive)

	table.FlushDB()

	require.Equal(t, 0, table.index.Len())
	require.Zero(t, table.indexStaleCount)
	require.False(t, table.indexCompactActive)
	require.Equal(t, hashTableKey{}, table.indexCompactAfter)
}
