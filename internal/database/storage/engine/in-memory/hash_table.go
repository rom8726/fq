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

func (s *HashTable) Clean(ctx context.Context) {
	s.m.Range(func(k, v interface{}) bool {
		select {
		case <-ctx.Done():
			return false
		default:
			elem := v.(*FqElem)
			if isExpiredWithDelta(elem.lastTxAt, elem.batchSize) {
				s.m.Delete(k)
			}

			return true
		}
	})
}

func (s *HashTable) Dump(ctx context.Context, dumpTx database.Tx, ch chan<- database.DumpElem) {
	s.m.Range(func(k, v interface{}) bool {
		select {
		case <-ctx.Done():
			return false
		default:
			elem := v.(*FqElem)
			if isExpired(elem.lastTxAt, elem.batchSize) {
				return true
			}

			key := k.(hashTableKey)
			value, txAt, tx := elem.DumpValue(dumpTx)

			ch <- database.DumpElem{
				Key:       key.key,
				BatchSize: key.batchSize,
				Value:     value,
				TxAt:      txAt,
				Tx:        tx,
			}

			return true
		}
	})
}

func (s *HashTable) RestoreDumpElem(elem database.DumpElem) {
	fqElem := NewFqElem(elem.BatchSize)
	fqElem.ver = elem.Tx
	fqElem.lastTxAt = elem.TxAt
	fqElem.value = elem.Value

	key := hashTableKey{key: elem.Key, batchSize: elem.BatchSize}
	s.m.Store(key, fqElem)
}

func (s *HashTable) getOrInitElem(key hashTableKey) *FqElem {
	v, ok := s.m.Load(key)
	if !ok {
		v, _ = s.m.LoadOrStore(key, NewFqElem(key.batchSize))
	}

	return v.(*FqElem)
}
