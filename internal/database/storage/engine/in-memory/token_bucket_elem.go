package inmemory

import (
	"fmt"
	"sync"

	"github.com/fq-db/fq/internal/database"
)

type TokenBucketElem struct {
	refillWindow database.TxTime
	tokens       database.ValueType
	lastRefillAt database.TxTime
	ver          database.Tx
	mu           sync.RWMutex
}

func NewTokenBucketElem(refillWindow uint32) *TokenBucketElem {
	return &TokenBucketElem{
		refillWindow: database.TxTime(refillWindow),
		ver:          database.NoTx,
	}
}

func (e *TokenBucketElem) RLimit(
	txCtx database.TxContext,
	capacity database.ValueType,
	refillAmount database.ValueType,
	beforeApply func() error,
) (database.RateLimitResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.refillLocked(txCtx.CurrTime, capacity, refillAmount)
	previous := usedCapacity(e.tokens, capacity)
	if e.tokens <= 0 {
		return e.makeResultLocked(txCtx.CurrTime, capacity, false), nil
	}

	if beforeApply != nil {
		if err := beforeApply(); err != nil {
			return database.RateLimitResult{}, fmt.Errorf("before apply token-bucket rate limit event: %w", err)
		}
	}

	e.consumeLocked(txCtx)

	result := e.makeResultLocked(txCtx.CurrTime, capacity, true)
	result.LimitFilled = previous < capacity && result.Current >= capacity

	return result, nil
}

func (e *TokenBucketElem) AddEvent(
	txCtx database.TxContext,
	capacity database.ValueType,
	refillAmount database.ValueType,
) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.refillLocked(txCtx.CurrTime, capacity, refillAmount)
	if e.tokens > 0 {
		e.consumeLocked(txCtx)
	}
}

func (e *TokenBucketElem) DumpValue(dumpTx database.Tx) (database.ValueType, database.TxTime, database.Tx) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.ver == database.NoTx || e.ver > dumpTx {
		return database.ErrorValue, 0, 0
	}

	return e.tokens, e.lastRefillAt, e.ver
}

func (e *TokenBucketElem) Restore(elem database.DumpElem) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.tokens = elem.Value
	e.lastRefillAt = elem.TxAt
	e.ver = elem.Tx
}

func (e *TokenBucketElem) refillLocked(
	now database.TxTime,
	capacity database.ValueType,
	refillAmount database.ValueType,
) {
	if e.ver == database.NoTx || e.lastRefillAt == 0 {
		e.tokens = capacity
		e.lastRefillAt = now

		return
	}

	if e.tokens > capacity {
		e.tokens = capacity
	}

	if e.tokens == capacity {
		e.lastRefillAt = now

		return
	}

	if now <= e.lastRefillAt {
		return
	}

	periods := (now - e.lastRefillAt) / e.refillWindow
	if periods == 0 {
		return
	}

	refilledTokens := int64(e.tokens) + int64(periods)*int64(refillAmount)
	if refilledTokens >= int64(capacity) {
		e.tokens = capacity
		e.lastRefillAt = now

		return
	}

	e.tokens = database.ValueType(refilledTokens)
	e.lastRefillAt += periods * e.refillWindow
}

func (e *TokenBucketElem) consumeLocked(txCtx database.TxContext) {
	e.tokens--
	e.ver = txCtx.Tx
	if e.lastRefillAt == 0 {
		e.lastRefillAt = txCtx.CurrTime
	}
}

func (e *TokenBucketElem) makeResultLocked(
	now database.TxTime,
	capacity database.ValueType,
	allowed bool,
) database.RateLimitResult {
	remaining := e.tokens
	if remaining < 0 {
		remaining = 0
	}
	if remaining > capacity {
		remaining = capacity
	}

	current := capacity - remaining
	if current < 0 {
		current = 0
	}

	resetAfter := uint32(0)
	if remaining == 0 {
		resetAfter = e.nextRefillAfterLocked(now)
	}

	return database.RateLimitResult{
		Allowed:    allowed,
		Current:    current,
		Remaining:  remaining,
		ResetAfter: resetAfter,
	}
}

func usedCapacity(tokens, capacity database.ValueType) database.ValueType {
	remaining := tokens
	if remaining < 0 {
		remaining = 0
	}
	if remaining > capacity {
		remaining = capacity
	}

	current := capacity - remaining
	if current < 0 {
		return 0
	}

	return current
}

func (e *TokenBucketElem) nextRefillAfterLocked(now database.TxTime) uint32 {
	if e.lastRefillAt == 0 {
		return uint32(e.refillWindow)
	}

	next := uint64(e.lastRefillAt) + uint64(e.refillWindow)
	if next <= uint64(now) {
		return 0
	}

	return uint32(next - uint64(now))
}
