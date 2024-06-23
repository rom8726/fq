package inmemory

import (
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
	m sync.Map
}

func NewHashTable() *HashTable {
	return &HashTable{}
}

func (s *HashTable) Incr(txCtx database.TxContext, key database.BatchKey) database.ValueType {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	v := s.getOrInitElem(htKey)

	return v.Incr(txCtx)
}

func (s *HashTable) Get(key database.BatchKey) (database.ValueType, bool) {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	v := s.getOrInitElem(htKey)

	return v.Value(), false
}

func (s *HashTable) Del(key database.BatchKey) bool {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	_, ok := s.m.LoadAndDelete(htKey)

	return ok
}

func (s *HashTable) getOrInitElem(key hashTableKey) *FqElem {
	v, ok := s.m.Load(key)
	if !ok {
		v, _ = s.m.LoadOrStore(key, NewFqElem(key.batchSize))
	}

	return v.(*FqElem)
}
