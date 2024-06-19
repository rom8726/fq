package inmemory

import (
	"sync"
	"time"

	"fq/internal/database"
)

var HashTableBuilder = func(sz int) hashTable {
	return NewHashTable(sz)
}

type hashTableKey struct {
	key       string
	batchSize uint32
}

type HashTable struct {
	mu   sync.RWMutex
	data map[hashTableKey]*FqElem
}

func NewHashTable(sz int) *HashTable {
	return &HashTable{
		data: make(map[hashTableKey]*FqElem, sz),
	}
}

func (s *HashTable) Incr(txCtx database.TxContext, key database.BatchKey) database.ValueType {
	if txCtx.FromWAL && isExpired(txCtx.CurrTime, key.BatchSize) {
		// expired value
		return 0 // return 0 for WAL worker
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	v, ok := s.data[htKey]
	if !ok {
		v = NewFqElem(key.BatchSize)
		s.data[htKey] = v
	}

	return v.Incr(txCtx)
}

func (s *HashTable) Get(key database.BatchKey) (database.ValueType, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	value, found := s.data[htKey]
	if found {
		return value.Value(), true
	}

	return 0, false
}

func isExpired(currTime database.TxTime, batchSize uint32) bool {
	batchEndsAt := uint32(currTime)/batchSize*batchSize + batchSize - 1

	return uint32(time.Now().Unix()) > batchEndsAt
}
