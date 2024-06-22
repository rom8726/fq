package inmemory

import (
	"sync"
	"time"

	"fq/internal/database"
)

var HashTableBuilder = func(sz, cleanPauseThreshold int, cleanPause time.Duration) hashTable {
	return NewHashTable(sz, cleanPauseThreshold, cleanPause)
}

type hashTableKey struct {
	key       string
	batchSize uint32
}

type HashTable struct {
	mu   sync.RWMutex
	data map[hashTableKey]*FqElem

	cleanPauseThreshold int
	cleanPause          time.Duration
}

func NewHashTable(sz, cleanPauseThreshold int, cleanPause time.Duration) *HashTable {
	return &HashTable{
		data:                make(map[hashTableKey]*FqElem, sz),
		cleanPauseThreshold: cleanPauseThreshold,
		cleanPause:          cleanPause,
	}
}

func (s *HashTable) Clean() {
	s.mu.Lock()
	defer s.mu.Unlock()

	cnt := 0

	for k, v := range s.data {
		cnt++

		_, expiredWithDelta := isExpiredWithDelta(v.lastTxAt, v.batchSize)
		if expiredWithDelta {
			delete(s.data, k)
		}

		if cnt >= s.cleanPauseThreshold {
			cnt = 0

			s.mu.Unlock()
			time.Sleep(s.cleanPause)
			s.mu.Lock()
		}
	}
}

func (s *HashTable) Incr(txCtx database.TxContext, key database.BatchKey) database.ValueType {
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

func (s *HashTable) Del(key database.BatchKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	_, found := s.data[htKey]
	if found {
		delete(s.data, htKey)

		return true
	}

	return false
}
