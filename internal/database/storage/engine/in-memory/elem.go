package inmemory

import (
	"sync"

	"fq/internal/database"
)

type Elem struct {
	ver   database.Tx
	value database.ValueType

	dumpVer   database.Tx
	dumpValue database.ValueType

	mu sync.Mutex
}

func NewElem() *Elem {
	return &Elem{
		ver:     database.NoTx,
		dumpVer: database.NoTx,
	}
}

func (e *Elem) Incr(tx, dumpTx database.Tx) database.ValueType {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.dumpVer < dumpTx {
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

	return e.value
}

func (e *Elem) Value() database.ValueType {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.value
}

func (e *Elem) DumpValue(dumpTx database.Tx) database.ValueType {
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
