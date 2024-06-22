package inmemory

type CappingsHolder struct {
	capping      uint32
	tablesHolder map[uint64]*hashTable
}

func newCappingsHolder(capping uint32) *CappingsHolder {
	return &CappingsHolder{
		capping:      capping,
		tablesHolder: make(map[uint64]*hashTable, 2),
	}
}
