package inmemory

import (
	"sync"

	"fq/internal/database"
)

type FqElem struct {
	ver      database.Tx
	value    database.ValueType
	lastTxAt database.TxTime

	dumpVer      database.Tx
	dumpValue    database.ValueType
	dumpLastTxAt database.TxTime

	batchSize database.TxTime
	mu        sync.Mutex
}

func NewFqElem(batchSize uint32) *FqElem {
	return &FqElem{
		batchSize: database.TxTime(batchSize),
		ver:       database.NoTx,
		dumpVer:   database.NoTx,
	}
}

func (e *FqElem) Incr(txCtx database.TxContext) database.ValueType {
	batchStartsAt := txCtx.CurrTime / e.batchSize * e.batchSize

	e.mu.Lock()
	defer e.mu.Unlock()

	value := e.value
	if e.lastTxAt < batchStartsAt {
		value = 0
	}

	if e.dumpVer != txCtx.DumpTx {
		if txCtx.Tx == txCtx.DumpTx {
			e.dumpValue = value + 1
			e.dumpVer = txCtx.Tx
			e.dumpLastTxAt = txCtx.CurrTime
		} else {
			e.dumpValue = e.value
			e.dumpVer = e.ver
			e.dumpLastTxAt = e.lastTxAt
		}
	}

	e.value = value + 1
	e.ver = txCtx.Tx
	e.lastTxAt = txCtx.CurrTime

	return e.value
}

func (e *FqElem) Value() database.ValueType {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.value
}

func (e *FqElem) DumpValue(dumpTx database.Tx) (database.ValueType, database.TxTime) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.ver <= dumpTx {
		return e.value, e.lastTxAt
	}

	if e.dumpVer <= dumpTx {
		return e.dumpValue, e.dumpLastTxAt
	}

	return database.ErrorValue, 0
}
