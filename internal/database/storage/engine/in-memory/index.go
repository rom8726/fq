package inmemory

import (
	"sync"

	"github.com/ancientlore/go-avltree/v2"

	"fq/internal/database"
)

type Index struct {
	tree *avltree.Map[database.BatchKey, *FqElem]
	mu   sync.RWMutex
}

func newIndex() *Index {
	return &Index{
		tree: avltree.NewMap[database.BatchKey, *FqElem](compareKeys),
	}
}

func (i *Index) Add(key database.BatchKey, elem *FqElem) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.tree.Add(key, elem)
}

func (i *Index) Del(key database.BatchKey) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.tree.Remove(key)

	i.tree.Iter()
}

func compareKeys(k1 database.BatchKey, k2 database.BatchKey) int {
	switch {
	case k1 == k2:
		return 0
	case k1.Key < k2.Key ||
		(k1.Key == k2.Key && k1.BatchSize < k2.BatchSize):
		return -1
	default:
		return 1
	}
}
