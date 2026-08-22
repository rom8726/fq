package inmemory

import (
	"context"
	"sync"
	"time"

	"fq/internal/database"
)

var HashTableBuilder = func() hashTable {
	return NewHashTable()
}

type hashTableKey struct {
	key       string
	batchSize uint32
}

type HashTable struct {
	mu sync.RWMutex
	m  map[hashTableKey]*FqElem
	sw map[hashTableKey]*SlidingWindowElem
	tb map[hashTableKey]*TokenBucketElem
}

func NewHashTable() *HashTable {
	return &HashTable{
		m:  make(map[hashTableKey]*FqElem),
		sw: make(map[hashTableKey]*SlidingWindowElem),
		tb: make(map[hashTableKey]*TokenBucketElem),
	}
}

func (s *HashTable) Incr(txCtx database.TxContext, key database.BatchKey) database.ValueType {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	v := s.getOrInitElem(htKey)

	return v.Incr(txCtx)
}

func (s *HashTable) RLimitFixedWindow(
	txCtx database.TxContext,
	key database.BatchKey,
	limit database.ValueType,
	beforeApply func() error,
) (database.RateLimitResult, error) {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	v := s.getOrInitElem(htKey)

	return v.RLimitFixedWindow(txCtx, limit, beforeApply)
}

func (s *HashTable) RLimitSlidingWindow(
	txCtx database.TxContext,
	key database.BatchKey,
	limit database.ValueType,
	beforeApply func() error,
) (database.RateLimitResult, error) {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	v := s.getOrInitSlidingWindowElem(htKey)

	return v.RLimit(txCtx, limit, beforeApply)
}

func (s *HashTable) RLimitTokenBucket(
	txCtx database.TxContext,
	key database.BatchKey,
	capacity database.ValueType,
	refillAmount database.ValueType,
	beforeApply func() error,
) (database.RateLimitResult, error) {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	v := s.getOrInitTokenBucketElem(htKey)

	return v.RLimit(txCtx, capacity, refillAmount, beforeApply)
}

func (s *HashTable) AddSlidingWindowEvent(txCtx database.TxContext, key database.BatchKey) {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	v := s.getOrInitSlidingWindowElem(htKey)

	v.AddEvent(txCtx)
}

func (s *HashTable) AddTokenBucketEvent(
	txCtx database.TxContext,
	key database.BatchKey,
	capacity database.ValueType,
	refillAmount database.ValueType,
) {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	v := s.getOrInitTokenBucketElem(htKey)

	v.AddEvent(txCtx, capacity, refillAmount)
}

func (s *HashTable) Get(key database.BatchKey) (database.ValueType, bool) {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}

	s.mu.RLock()
	v, ok := s.m[htKey]
	s.mu.RUnlock()

	if !ok {
		return 0, false
	}

	return v.Value(), true
}

func (s *HashTable) Del(key database.BatchKey) bool {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}

	s.mu.Lock()
	_, counterFound := s.m[htKey]
	if counterFound {
		delete(s.m, htKey)
	}
	_, slidingWindowFound := s.sw[htKey]
	if slidingWindowFound {
		delete(s.sw, htKey)
	}
	_, tokenBucketFound := s.tb[htKey]
	if tokenBucketFound {
		delete(s.tb, htKey)
	}
	s.mu.Unlock()

	return counterFound || slidingWindowFound || tokenBucketFound
}

func (s *HashTable) Clean(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keysToDelete := make([]hashTableKey, 0, len(s.m)/10) // Pre-allocate for ~10% deletions

	for k, v := range s.m {
		select {
		case <-ctx.Done():
			return
		default:
			if isExpiredWithDelta(v.lastTxAt, v.batchSize) {
				keysToDelete = append(keysToDelete, k)
			}
		}
	}

	for _, k := range keysToDelete {
		delete(s.m, k)
	}

	now := database.TxTime(time.Now().Unix())
	for k, v := range s.sw {
		select {
		case <-ctx.Done():
			return
		default:
			if v.Clean(now) {
				delete(s.sw, k)
			}
		}
	}
}

func (s *HashTable) Dump(ctx context.Context, dumpTx database.Tx, ch chan<- database.DumpElem) {
	s.mu.RLock()
	// Create a snapshot to avoid holding lock during channel operations
	items := make([]struct {
		key  hashTableKey
		elem *FqElem
	}, 0, len(s.m))
	for k, v := range s.m {
		items = append(items, struct {
			key  hashTableKey
			elem *FqElem
		}{k, v})
	}
	swItems := make([]struct {
		key  hashTableKey
		elem *SlidingWindowElem
	}, 0, len(s.sw))
	for k, v := range s.sw {
		swItems = append(swItems, struct {
			key  hashTableKey
			elem *SlidingWindowElem
		}{k, v})
	}
	tbItems := make([]struct {
		key  hashTableKey
		elem *TokenBucketElem
	}, 0, len(s.tb))
	for k, v := range s.tb {
		tbItems = append(tbItems, struct {
			key  hashTableKey
			elem *TokenBucketElem
		}{k, v})
	}
	s.mu.RUnlock()

	for _, item := range items {
		select {
		case <-ctx.Done():
			return
		default:
			if isExpired(item.elem.lastTxAt, item.elem.batchSize) {
				continue
			}

			value, txAt, tx := item.elem.DumpValue(dumpTx)

			ch <- database.DumpElem{
				Kind:      database.DumpElemKindCounter,
				Key:       item.key.key,
				BatchSize: item.key.batchSize,
				Value:     value,
				TxAt:      txAt,
				Tx:        tx,
			}
		}
	}

	for _, item := range swItems {
		item.elem.Dump(ctx.Done(), item.key, dumpTx, ch)
	}

	for _, item := range tbItems {
		value, txAt, tx := item.elem.DumpValue(dumpTx)
		if value == database.ErrorValue {
			continue
		}

		select {
		case <-ctx.Done():
			return
		default:
		}

		ch <- database.DumpElem{
			Kind:      database.DumpElemKindTokenBucket,
			Key:       item.key.key,
			BatchSize: item.key.batchSize,
			Value:     value,
			TxAt:      txAt,
			Tx:        tx,
		}
	}
}

func (s *HashTable) RestoreDumpElem(elem database.DumpElem) {
	if elem.Kind == database.DumpElemKindSlidingWindowBucket {
		key := hashTableKey{key: elem.Key, batchSize: elem.BatchSize}
		swElem := s.getOrInitSlidingWindowElem(key)
		swElem.RestoreBucket(elem)

		return
	}

	if elem.Kind == database.DumpElemKindTokenBucket {
		key := hashTableKey{key: elem.Key, batchSize: elem.BatchSize}
		tbElem := s.getOrInitTokenBucketElem(key)
		tbElem.Restore(elem)

		return
	}

	fqElem := NewFqElem(elem.BatchSize)
	fqElem.ver = elem.Tx
	fqElem.lastTxAt = elem.TxAt
	fqElem.value = elem.Value

	key := hashTableKey{key: elem.Key, batchSize: elem.BatchSize}

	s.mu.Lock()
	s.m[key] = fqElem
	s.mu.Unlock()
}

func (s *HashTable) getOrInitElem(key hashTableKey) *FqElem {
	// Fast path: try read lock first
	s.mu.RLock()
	v, ok := s.m[key]
	s.mu.RUnlock()

	if ok {
		return v
	}

	// Slow path: need to create, use write lock
	s.mu.Lock()
	// Double-check after acquiring write lock
	v, ok = s.m[key]
	if !ok {
		v = NewFqElem(key.batchSize)
		s.m[key] = v
	}
	s.mu.Unlock()

	return v
}

func (s *HashTable) getOrInitSlidingWindowElem(key hashTableKey) *SlidingWindowElem {
	s.mu.RLock()
	v, ok := s.sw[key]
	s.mu.RUnlock()

	if ok {
		return v
	}

	s.mu.Lock()
	v, ok = s.sw[key]
	if !ok {
		v = NewSlidingWindowElem(key.batchSize)
		s.sw[key] = v
	}
	s.mu.Unlock()

	return v
}

func (s *HashTable) getOrInitTokenBucketElem(key hashTableKey) *TokenBucketElem {
	s.mu.RLock()
	v, ok := s.tb[key]
	s.mu.RUnlock()

	if ok {
		return v
	}

	s.mu.Lock()
	v, ok = s.tb[key]
	if !ok {
		v = NewTokenBucketElem(key.batchSize)
		s.tb[key] = v
	}
	s.mu.Unlock()

	return v
}
