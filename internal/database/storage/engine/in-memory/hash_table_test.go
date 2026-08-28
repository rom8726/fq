package inmemory

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database"
)

func TestHashTableCleanRemovesExpiredCountersInChunks(t *testing.T) {
	table := NewHashTable()
	now := database.TxTime(time.Now().Unix())

	for i := 0; i < cleanChunkSize+1; i++ {
		key := hashTableKey{key: fmt.Sprintf("expired-%d", i), batchSize: 1}
		elem := NewFqElem(key.batchSize)
		elem.lastTxAt = now - expireDelta - 2
		elem.value = 1
		table.m[key] = elem
	}

	activeKey := hashTableKey{key: "active", batchSize: 60}
	activeElem := NewFqElem(activeKey.batchSize)
	activeElem.lastTxAt = now
	activeElem.value = 1
	table.m[activeKey] = activeElem

	table.Clean(context.Background())

	require.Len(t, table.m, 1)
	require.Contains(t, table.m, activeKey)
}

func TestHashTableCleanRemovesEmptySlidingWindows(t *testing.T) {
	table := NewHashTable()
	now := database.TxTime(time.Now().Unix())

	key := hashTableKey{key: "expired-window", batchSize: 1}
	elem := NewSlidingWindowElem(key.batchSize)
	elem.RestoreBucket(database.DumpElem{
		Value: 1,
		TxAt:  now - 2,
		Tx:    1,
	})
	table.sw[key] = elem

	table.Clean(context.Background())

	require.Empty(t, table.sw)
}

func TestHashTableCleanHonorsCanceledContext(t *testing.T) {
	table := NewHashTable()
	now := database.TxTime(time.Now().Unix())
	key := hashTableKey{key: "expired", batchSize: 1}
	elem := NewFqElem(key.batchSize)
	elem.lastTxAt = now - expireDelta - 2
	elem.value = 1
	table.m[key] = elem

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	table.Clean(ctx)

	require.Contains(t, table.m, key)
}
