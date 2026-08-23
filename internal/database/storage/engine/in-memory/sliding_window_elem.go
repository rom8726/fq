package inmemory

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"fq/internal/database"
)

type slidingWindowBucket struct {
	at    database.TxTime
	parts []slidingWindowBucketPart
}

type slidingWindowBucketPart struct {
	tx    database.Tx
	count database.ValueType
}

type SlidingWindowElem struct {
	window  database.TxTime
	buckets map[database.TxTime]slidingWindowBucket
	mu      sync.RWMutex
}

func NewSlidingWindowElem(window uint32) *SlidingWindowElem {
	return &SlidingWindowElem{
		window:  database.TxTime(window),
		buckets: make(map[database.TxTime]slidingWindowBucket),
	}
}

func (e *SlidingWindowElem) RLimit(
	txCtx database.TxContext,
	limit database.ValueType,
	beforeApply func() error,
) (database.RateLimitResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.pruneLocked(txCtx.CurrTime)
	previous, oldest := e.currentLocked()
	resetAfter := e.resetAfter(txCtx.CurrTime, oldest)

	if previous >= limit {
		return database.RateLimitResult{
			Allowed:    false,
			Current:    previous,
			Remaining:  0,
			ResetAfter: resetAfter,
		}, nil
	}

	if beforeApply != nil {
		if err := beforeApply(); err != nil {
			return database.RateLimitResult{}, fmt.Errorf("before apply sliding-window rate limit event: %w", err)
		}
	}

	current := e.addEventLocked(txCtx.CurrTime, txCtx.Tx)
	_, oldest = e.currentLocked()
	resetAfter = e.resetAfter(txCtx.CurrTime, oldest)
	remaining := limit - current
	if remaining < 0 {
		remaining = 0
	}

	return database.RateLimitResult{
		Allowed:     true,
		Current:     current,
		Remaining:   remaining,
		ResetAfter:  resetAfter,
		LimitFilled: previous < limit && current >= limit,
	}, nil
}

func (e *SlidingWindowElem) AddEvent(txCtx database.TxContext) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.pruneLocked(txCtx.CurrTime)
	e.addEventLocked(txCtx.CurrTime, txCtx.Tx)
}

func (e *SlidingWindowElem) Clean(now database.TxTime) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.pruneLocked(now)

	return len(e.buckets) == 0
}

func (e *SlidingWindowElem) Dump(
	ctxDone <-chan struct{},
	key hashTableKey,
	dumpTx database.Tx,
	ch chan<- database.DumpElem,
) {
	now := database.TxTime(0)
	e.mu.RLock()
	buckets := make([]slidingWindowBucket, 0, len(e.buckets))
	for _, bucket := range e.buckets {
		if now == 0 {
			now = database.TxTime(time.Now().Unix())
		}
		if bucket.at+e.window <= now {
			continue
		}
		dumpBucket := slidingWindowBucket{at: bucket.at}
		for _, part := range bucket.parts {
			if part.tx <= dumpTx {
				dumpBucket.parts = append(dumpBucket.parts, part)
			}
		}
		if len(dumpBucket.parts) > 0 {
			buckets = append(buckets, dumpBucket)
		}
	}
	e.mu.RUnlock()

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].at < buckets[j].at
	})

	for _, bucket := range buckets {
		count, tx := bucket.countAndMaxTx()
		if count <= 0 {
			continue
		}

		select {
		case <-ctxDone:
			return
		default:
		}

		ch <- database.DumpElem{
			Kind:      database.DumpElemKindSlidingWindowBucket,
			Key:       key.key,
			BatchSize: key.batchSize,
			Value:     count,
			TxAt:      bucket.at,
			Tx:        tx,
		}
	}
}

func (e *SlidingWindowElem) RestoreBucket(elem database.DumpElem) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if elem.Value <= 0 {
		return
	}

	bucket := e.buckets[elem.TxAt]
	bucket.at = elem.TxAt
	bucket.addPart(elem.Tx, elem.Value)
	e.buckets[elem.TxAt] = bucket
}

func (e *SlidingWindowElem) addEventLocked(at database.TxTime, tx database.Tx) database.ValueType {
	bucket := e.buckets[at]
	bucket.at = at
	bucket.addPart(tx, 1)
	e.buckets[at] = bucket

	current, _ := e.currentLocked()
	return current
}

func (e *SlidingWindowElem) currentLocked() (database.ValueType, database.TxTime) {
	var current database.ValueType
	var oldest database.TxTime
	for at, bucket := range e.buckets {
		current += bucket.count()
		if oldest == 0 || at < oldest {
			oldest = at
		}
	}

	return current, oldest
}

func (e *SlidingWindowElem) resetAfter(now, oldest database.TxTime) uint32 {
	if oldest == 0 {
		return uint32(e.window)
	}

	expiresAt := oldest + e.window
	if expiresAt <= now {
		return 0
	}

	return uint32(expiresAt - now)
}

func (e *SlidingWindowElem) pruneLocked(now database.TxTime) {
	for at := range e.buckets {
		if at+e.window <= now {
			delete(e.buckets, at)
		}
	}
}

func (b *slidingWindowBucket) addPart(tx database.Tx, count database.ValueType) {
	if count <= 0 {
		return
	}
	for i := range b.parts {
		if b.parts[i].tx == tx {
			b.parts[i].count += count
			return
		}
	}

	b.parts = append(b.parts, slidingWindowBucketPart{tx: tx, count: count})
}

func (b slidingWindowBucket) count() database.ValueType {
	count, _ := b.countAndMaxTx()

	return count
}

func (b slidingWindowBucket) countAndMaxTx() (database.ValueType, database.Tx) {
	var count database.ValueType
	var maxTx database.Tx
	for _, part := range b.parts {
		count += part.count
		if part.tx > maxTx {
			maxTx = part.tx
		}
	}

	return count, maxTx
}
