package inmemory

import (
	"context"
	"sync"

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
}

func NewHashTable() *HashTable {
	return &HashTable{
		m: make(map[hashTableKey]*FqElem),
	}
}

func (s *HashTable) Incr(txCtx database.TxContext, key database.BatchKey) database.ValueType {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	v := s.getOrInitElem(htKey)

	return v.Incr(txCtx)
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
	_, ok := s.m[htKey]
	if ok {
		delete(s.m, htKey)
	}
	s.mu.Unlock()

	return ok
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
				Key:       item.key.key,
				BatchSize: item.key.batchSize,
				Value:     value,
				TxAt:      txAt,
				Tx:        tx,
			}
		}
	}
}

func (s *HashTable) RestoreDumpElem(elem database.DumpElem) {
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
