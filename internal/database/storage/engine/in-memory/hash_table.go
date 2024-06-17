package inmemory

import (
	"sync"

	"fq/internal/database"
)

var HashTableBuilder = func(sz int) hashTable {
	return NewHashTable(sz)
}

type HashTable struct {
	mu   sync.RWMutex
	data map[database.BatchKey]*FqElem
}

func NewHashTable(sz int) *HashTable {
	return &HashTable{
		data: make(map[database.BatchKey]*FqElem, sz),
	}
}

func (s *HashTable) Incr(txCtx database.TxContext, key database.BatchKey) database.ValueType {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.data[key]
	if !ok {
		v = NewFqElem(key.BatchSize)
		s.data[key] = v
	}

	return v.Incr(txCtx)
}

func (s *HashTable) Get(key database.BatchKey) (database.ValueType, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, found := s.data[key]
	if found {
		return value.Value(), true
	}

	return 0, false
}
