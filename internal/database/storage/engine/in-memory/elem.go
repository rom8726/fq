package inmemory

import (
	"sync"
	"time"

	"fq/internal/database"
)

type FqElem struct {
	ver   database.Tx
	value database.ValueType

	dumpVer   database.Tx
	dumpValue database.ValueType

	batchSize uint32
	lastTxAt  uint32

	mu sync.Mutex
}

func NewFqElem(batchSize uint32) *FqElem {
	return &FqElem{
		batchSize: batchSize,
		ver:       database.NoTx,
		dumpVer:   database.NoTx,
	}
}

func (e *FqElem) Incr(tx, dumpTx database.Tx) database.ValueType {
	now := uint32(time.Now().Unix())
	batchStartsAt := now / e.batchSize * e.batchSize

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.lastTxAt < batchStartsAt {
		e.value = 0
	}

	if e.dumpVer != dumpTx {
		if tx == dumpTx {
			e.dumpValue = e.value + 1
			e.dumpVer = tx
		} else {
			e.dumpValue = e.value
			e.dumpVer = e.ver
		}
	}

	e.value++
	e.ver = tx
	e.lastTxAt = now

	return e.value
}

func (e *FqElem) Value() database.ValueType {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.value
}

func (e *FqElem) DumpValue(dumpTx database.Tx) database.ValueType {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.ver <= dumpTx {
		return e.value
	}

	if e.dumpVer <= dumpTx {
		return e.dumpValue
	}

	return database.ErrorValue
}
