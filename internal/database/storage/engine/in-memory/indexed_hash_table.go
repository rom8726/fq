package inmemory

import "fq/internal/database"

type IndexedHashTable struct {
	hashTable hashTable
	index     *Index
}

func newIndexedHashTable(tableBuilder func(sz int) hashTable, size int) *IndexedHashTable {
	return &IndexedHashTable{
		hashTable: tableBuilder(size),
		index:     newIndex(),
	}
}

func (it *IndexedHashTable) Incr(txCtx database.TxContext, key database.BatchKey) database.ValueType {
	v, elem := it.hashTable.Incr(txCtx, key)
	if elem != nil {
		it.index.Add(key, elem)
	}

	return v
}

func (it *IndexedHashTable) Get(key database.BatchKey) (database.ValueType, bool) {
	return it.hashTable.Get(key)
}

func (it *IndexedHashTable) Del(key database.BatchKey) bool {
	if it.hashTable.Del(key) {
		it.index.Del(key)

		return true
	}

	return false
}
