package inmemory

import (
	"sync"
	"time"

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
	mu        sync.RWMutex
}

func NewFqElem(batchSize uint32) *FqElem {
	return &FqElem{
		batchSize: database.TxTime(batchSize),
		ver:       database.NoTx,
		dumpVer:   database.NoTx,
	}
}

func (e *FqElem) Incr(txCtx database.TxContext) database.ValueType {
	batchStartsAt := startOfBatch(txCtx.CurrTime, e.batchSize)

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
	now := time.Now().Unix()
	batchStartsAt := startOfBatch(database.TxTime(now), e.batchSize)

	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.lastTxAt < batchStartsAt {
		return 0
	}

	return e.value
}

func (e *FqElem) DumpValue(dumpTx database.Tx) (database.ValueType, database.TxTime, database.Tx) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.ver <= dumpTx {
		return e.value, e.lastTxAt, e.ver
	}

	if e.dumpVer <= dumpTx {
		return e.dumpValue, e.dumpLastTxAt, e.dumpVer
	}

	return database.ErrorValue, 0, 0
}
