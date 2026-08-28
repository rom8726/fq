package inmemory

import (
	"fmt"
	"sync"
	"time"

	"github.com/fq-db/fq/internal/database"
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

	return e.applyIncrementLocked(txCtx, value)
}

func (e *FqElem) RLimitFixedWindow(
	txCtx database.TxContext,
	limit database.ValueType,
	beforeApply func() error,
) (database.RateLimitResult, error) {
	batchStartsAt := startOfBatch(txCtx.CurrTime, e.batchSize)
	batchEndsAt := endOfBatch(txCtx.CurrTime, e.batchSize)
	resetAfter := uint32(0)
	if batchEndsAt >= txCtx.CurrTime {
		resetAfter = uint32(batchEndsAt - txCtx.CurrTime + 1)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	value := e.value
	if e.lastTxAt < batchStartsAt {
		value = 0
	}

	if value >= limit {
		return database.RateLimitResult{
			Allowed:    false,
			Current:    value,
			Remaining:  0,
			ResetAfter: resetAfter,
		}, nil
	}

	if beforeApply != nil {
		if err := beforeApply(); err != nil {
			return database.RateLimitResult{}, fmt.Errorf("before apply rate limit increment: %w", err)
		}
	}

	current := e.applyIncrementLocked(txCtx, value)
	remaining := limit - current
	if remaining < 0 {
		remaining = 0
	}

	return database.RateLimitResult{
		Allowed:     true,
		Current:     current,
		Remaining:   remaining,
		ResetAfter:  resetAfter,
		LimitFilled: value < limit && current >= limit,
	}, nil
}

func (e *FqElem) applyIncrementLocked(txCtx database.TxContext, value database.ValueType) database.ValueType {
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

func (e *FqElem) ExpiredWithDelta(now database.TxTime) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return now > (endOfBatch(e.lastTxAt, e.batchSize) + expireDelta)
}

func (e *FqElem) DumpValue(dumpTx database.Tx) (database.ValueType, database.TxTime, database.Tx) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if isExpired(e.lastTxAt, e.batchSize) {
		return database.ErrorValue, 0, 0
	}

	if e.ver <= dumpTx {
		return e.value, e.lastTxAt, e.ver
	}

	if e.dumpVer <= dumpTx {
		return e.dumpValue, e.dumpLastTxAt, e.dumpVer
	}

	return database.ErrorValue, 0, 0
}
