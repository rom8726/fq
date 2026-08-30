package inmemory

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/fq-db/fq/internal/database"
)

var HashTableBuilder = func() hashTable {
	return NewHashTable()
}

const (
	cleanChunkSize       = 1024
	cleanChunkTimeBudget = 2 * time.Millisecond
)

type hashTableKey struct {
	key       string
	batchSize uint32
}

type HashTable struct {
	mu sync.RWMutex
	m  map[hashTableKey]*FqElem
	sw map[hashTableKey]*SlidingWindowElem
	tb map[hashTableKey]*TokenBucketElem
	q  map[string]*QuotaElem
}

func NewHashTable() *HashTable {
	return &HashTable{
		m:  make(map[hashTableKey]*FqElem),
		sw: make(map[hashTableKey]*SlidingWindowElem),
		tb: make(map[hashTableKey]*TokenBucketElem),
		q:  make(map[string]*QuotaElem),
	}
}

func (s *HashTable) Incr(txCtx database.TxContext, key database.BatchKey) database.ValueType {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	v := s.getOrInitElem(htKey)

	return v.Incr(txCtx)
}

func (s *HashTable) RLimitFixedWindow(
	txCtx database.TxContext,
	key database.BatchKey,
	limit database.ValueType,
	beforeApply func() error,
) (database.RateLimitResult, error) {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	v := s.getOrInitElem(htKey)

	return v.RLimitFixedWindow(txCtx, limit, beforeApply)
}

func (s *HashTable) RLimitSlidingWindow(
	txCtx database.TxContext,
	key database.BatchKey,
	limit database.ValueType,
	beforeApply func() error,
) (database.RateLimitResult, error) {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	v := s.getOrInitSlidingWindowElem(htKey)

	return v.RLimit(txCtx, limit, beforeApply)
}

func (s *HashTable) RLimitTokenBucket(
	txCtx database.TxContext,
	key database.BatchKey,
	capacity database.ValueType,
	refillAmount database.ValueType,
	beforeApply func() error,
) (database.RateLimitResult, error) {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	v := s.getOrInitTokenBucketElem(htKey)

	return v.RLimit(txCtx, capacity, refillAmount, beforeApply)
}

func (s *HashTable) AddSlidingWindowEvent(txCtx database.TxContext, key database.BatchKey) {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	v := s.getOrInitSlidingWindowElem(htKey)

	v.AddEvent(txCtx)
}

func (s *HashTable) AddTokenBucketEvent(
	txCtx database.TxContext,
	key database.BatchKey,
	capacity database.ValueType,
	refillAmount database.ValueType,
) {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	v := s.getOrInitTokenBucketElem(htKey)

	v.AddEvent(txCtx, capacity, refillAmount)
}

func (s *HashTable) QuotaAcquire(
	txCtx database.TxContext,
	request database.QuotaAcquireRequest,
	beforeApply func() error,
) (database.QuotaAcquireResult, error) {
	switch request.Ownership {
	case database.QuotaOwnershipServer:
		s.mu.RLock()
		v, ok := s.q[request.Name]
		s.mu.RUnlock()
		if !ok {
			return database.QuotaAcquireResult{}, database.ErrQuotaNotFound
		}

		request.Limit = v.Limit()

		return v.Acquire(txCtx, request, beforeApply)
	case database.QuotaOwnershipClientLease:
		v := s.getOrInitQuotaElem(request.Name, request.Limit, database.QuotaOwnershipClientLease)

		return v.Acquire(txCtx, request, beforeApply)
	default:
		return database.QuotaAcquireResult{}, database.ErrQuotaOwnershipMismatch
	}

}

func (s *HashTable) QuotaSet(
	txCtx database.TxContext,
	name string,
	limit database.ValueType,
	beforeApply func() error,
) (bool, error) {
	v := s.getOrInitQuotaElem(name, 0, database.QuotaOwnershipServer)

	return v.SetLimit(txCtx, limit, beforeApply)
}

func (s *HashTable) QuotaRelease(
	txCtx database.TxContext,
	name, clientID string,
	beforeApply func() error,
) (database.QuotaReleaseResult, error) {
	s.mu.RLock()
	v, ok := s.q[name]
	s.mu.RUnlock()
	if !ok {
		return database.QuotaReleaseResult{}, nil
	}

	return v.Release(txCtx, clientID, beforeApply)
}

func (s *HashTable) QuotaDelete(txCtx database.TxContext, name string, beforeApply func() error) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.q[name]
	if !ok {
		return false, nil
	}
	if !v.CanDelete(txCtx.CurrTime) {
		return false, database.ErrQuotaNotEmpty
	}
	if beforeApply != nil {
		if err := beforeApply(); err != nil {
			return false, fmt.Errorf("before apply quota delete: %w", err)
		}
	}

	delete(s.q, name)

	return true, nil
}

func (s *HashTable) QuotaInfo(now database.TxTime, name string) database.QuotaInfo {
	s.mu.RLock()
	v, ok := s.q[name]
	s.mu.RUnlock()
	if !ok {
		return database.QuotaInfo{}
	}

	return v.Info(now)
}

func (s *HashTable) Get(key database.BatchKey) (database.ValueType, bool) {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}

	s.mu.RLock()
	v, ok := s.m[htKey]
	s.mu.RUnlock()

	if !ok {
		return 0, false
	}

	return v.Value(), true
}

func (s *HashTable) Del(key database.BatchKey) bool {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}

	s.mu.Lock()
	_, counterFound := s.m[htKey]
	if counterFound {
		delete(s.m, htKey)
	}
	_, slidingWindowFound := s.sw[htKey]
	if slidingWindowFound {
		delete(s.sw, htKey)
	}
	_, tokenBucketFound := s.tb[htKey]
	if tokenBucketFound {
		delete(s.tb, htKey)
	}
	s.mu.Unlock()

	return counterFound || slidingWindowFound || tokenBucketFound
}

func (s *HashTable) Clean(ctx context.Context) {
	now := database.TxTime(time.Now().Unix())

	counterItems, slidingWindowItems, quotaItems := s.cleanSnapshot(ctx)
	if ctx.Err() != nil {
		return
	}

	counterKeysToDelete := make([]cleanCounterItem, 0, len(counterItems)/10)
	for _, item := range counterItems {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if item.elem.ExpiredWithDelta(now) {
			counterKeysToDelete = append(counterKeysToDelete, item)
		}
	}

	s.deleteExpiredCounters(ctx, counterKeysToDelete, now)
	if ctx.Err() != nil {
		return
	}

	slidingWindowKeysToDelete := make([]cleanSlidingWindowItem, 0, len(slidingWindowItems)/10)
	for _, item := range slidingWindowItems {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if item.elem.Clean(now) {
			slidingWindowKeysToDelete = append(slidingWindowKeysToDelete, item)
		}
	}

	s.deleteEmptySlidingWindows(ctx, slidingWindowKeysToDelete, now)
	if ctx.Err() != nil {
		return
	}

	quotaKeysToDelete := make([]cleanQuotaItem, 0, len(quotaItems)/10)
	for _, item := range quotaItems {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if item.elem.Clean(now) {
			quotaKeysToDelete = append(quotaKeysToDelete, item)
		}
	}

	s.deleteEmptyQuotas(ctx, quotaKeysToDelete, now)
}

type cleanCounterItem struct {
	key  hashTableKey
	elem *FqElem
}

type cleanSlidingWindowItem struct {
	key  hashTableKey
	elem *SlidingWindowElem
}

type cleanQuotaItem struct {
	key  string
	elem *QuotaElem
}

func (s *HashTable) cleanSnapshot(ctx context.Context) (
	[]cleanCounterItem,
	[]cleanSlidingWindowItem,
	[]cleanQuotaItem,
) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	counterItems := make([]cleanCounterItem, 0, len(s.m))
	for k, v := range s.m {
		select {
		case <-ctx.Done():
			return nil, nil, nil
		default:
		}

		counterItems = append(counterItems, cleanCounterItem{key: k, elem: v})
	}

	slidingWindowItems := make([]cleanSlidingWindowItem, 0, len(s.sw))
	for k, v := range s.sw {
		select {
		case <-ctx.Done():
			return nil, nil, nil
		default:
		}

		slidingWindowItems = append(slidingWindowItems, cleanSlidingWindowItem{key: k, elem: v})
	}

	quotaItems := make([]cleanQuotaItem, 0, len(s.q))
	for k, v := range s.q {
		select {
		case <-ctx.Done():
			return nil, nil, nil
		default:
		}

		quotaItems = append(quotaItems, cleanQuotaItem{key: k, elem: v})
	}

	return counterItems, slidingWindowItems, quotaItems
}

//nolint:dupl // ok
func (s *HashTable) deleteExpiredCounters(ctx context.Context, items []cleanCounterItem, now database.TxTime) {
	for start := 0; start < len(items); {
		startedAt := time.Now()
		processed := 0

		s.mu.Lock()
		for ; start < len(items); start++ {
			if processed >= cleanChunkSize {
				break
			}
			if processed > 0 && time.Since(startedAt) >= cleanChunkTimeBudget {
				break
			}

			item := items[start]
			if current := s.m[item.key]; current == item.elem && current.ExpiredWithDelta(now) {
				delete(s.m, item.key)
			}
			processed++
		}
		s.mu.Unlock()

		if start >= len(items) {
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
			runtime.Gosched()
		}
	}
}

//nolint:dupl // ok
func (s *HashTable) deleteEmptySlidingWindows(
	ctx context.Context,
	items []cleanSlidingWindowItem,
	now database.TxTime,
) {
	for start := 0; start < len(items); {
		startedAt := time.Now()
		processed := 0

		s.mu.Lock()
		for ; start < len(items); start++ {
			if processed >= cleanChunkSize {
				break
			}
			if processed > 0 && time.Since(startedAt) >= cleanChunkTimeBudget {
				break
			}

			item := items[start]
			if current := s.sw[item.key]; current == item.elem && current.Clean(now) {
				delete(s.sw, item.key)
			}
			processed++
		}
		s.mu.Unlock()

		if start >= len(items) {
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
			runtime.Gosched()
		}
	}
}

//nolint:dupl // ok
func (s *HashTable) deleteEmptyQuotas(ctx context.Context, items []cleanQuotaItem, now database.TxTime) {
	for start := 0; start < len(items); {
		startedAt := time.Now()
		processed := 0

		s.mu.Lock()
		for ; start < len(items); start++ {
			if processed >= cleanChunkSize {
				break
			}
			if processed > 0 && time.Since(startedAt) >= cleanChunkTimeBudget {
				break
			}

			item := items[start]
			if current := s.q[item.key]; current == item.elem && current.Clean(now) {
				delete(s.q, item.key)
			}
			processed++
		}
		s.mu.Unlock()

		if start >= len(items) {
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
			runtime.Gosched()
		}
	}
}

func (s *HashTable) Dump(ctx context.Context, dumpTx database.Tx, ch chan<- database.DumpElem) {
	s.mu.RLock()
	// Create a snapshot to avoid holding lock during channel operations
	items := make([]struct {
		key  hashTableKey
		elem *FqElem
	}, 0, len(s.m))
	for k, v := range s.m {
		items = append(items, struct {
			key  hashTableKey
			elem *FqElem
		}{k, v})
	}
	swItems := make([]struct {
		key  hashTableKey
		elem *SlidingWindowElem
	}, 0, len(s.sw))
	for k, v := range s.sw {
		swItems = append(swItems, struct {
			key  hashTableKey
			elem *SlidingWindowElem
		}{k, v})
	}
	tbItems := make([]struct {
		key  hashTableKey
		elem *TokenBucketElem
	}, 0, len(s.tb))
	for k, v := range s.tb {
		tbItems = append(tbItems, struct {
			key  hashTableKey
			elem *TokenBucketElem
		}{k, v})
	}
	qItems := make([]struct {
		key  string
		elem *QuotaElem
	}, 0, len(s.q))
	for k, v := range s.q {
		qItems = append(qItems, struct {
			key  string
			elem *QuotaElem
		}{k, v})
	}
	s.mu.RUnlock()

	for _, item := range items {
		select {
		case <-ctx.Done():
			return
		default:
			value, txAt, tx := item.elem.DumpValue(dumpTx)
			if value == database.ErrorValue {
				continue
			}

			ch <- database.DumpElem{
				Kind:      database.DumpElemKindCounter,
				Key:       item.key.key,
				BatchSize: item.key.batchSize,
				Value:     value,
				TxAt:      txAt,
				Tx:        tx,
			}
		}
	}

	for _, item := range swItems {
		item.elem.Dump(ctx.Done(), item.key, dumpTx, ch)
	}

	for _, item := range tbItems {
		value, txAt, tx := item.elem.DumpValue(dumpTx)
		if value == database.ErrorValue {
			continue
		}

		select {
		case <-ctx.Done():
			return
		default:
		}

		ch <- database.DumpElem{
			Kind:      database.DumpElemKindTokenBucket,
			Key:       item.key.key,
			BatchSize: item.key.batchSize,
			Value:     value,
			TxAt:      txAt,
			Tx:        tx,
		}
	}

	now := database.TxTime(time.Now().Unix())
	for _, item := range qItems {
		if limit, ownership, tx, ok := item.elem.DumpConfig(dumpTx); ok {
			select {
			case <-ctx.Done():
				return
			default:
			}

			ch <- database.DumpElem{
				Kind:      database.DumpElemKindQuotaConfig,
				Key:       item.key,
				Limit:     limit,
				Ownership: ownership,
				Tx:        tx,
			}
		}

		limit := item.elem.Limit()
		for _, allocation := range item.elem.DumpAllocations(dumpTx, now) {
			select {
			case <-ctx.Done():
				return
			default:
			}

			ch <- database.DumpElem{
				Kind:      database.DumpElemKindQuotaAllocation,
				Key:       item.key,
				Limit:     limit,
				Value:     allocation.amount,
				Ownership: item.elem.Ownership(),
				ClientID:  allocation.clientID,
				ExpiresAt: allocation.expiresAt,
				TxAt:      allocation.txAt,
				Tx:        allocation.tx,
			}
		}
	}
}

func (s *HashTable) RestoreDumpElem(elem database.DumpElem) {
	if elem.Kind == database.DumpElemKindSlidingWindowBucket {
		key := hashTableKey{key: elem.Key, batchSize: elem.BatchSize}
		swElem := s.getOrInitSlidingWindowElem(key)
		swElem.RestoreBucket(elem)

		return
	}

	if elem.Kind == database.DumpElemKindTokenBucket {
		key := hashTableKey{key: elem.Key, batchSize: elem.BatchSize}
		tbElem := s.getOrInitTokenBucketElem(key)
		tbElem.Restore(elem)

		return
	}

	if elem.Kind == database.DumpElemKindQuotaAllocation {
		ownership := elem.Ownership
		if ownership == database.QuotaOwnershipUnknown {
			ownership = database.QuotaOwnershipClientLease
		}
		quotaElem := s.getOrInitQuotaElem(elem.Key, elem.Limit, ownership)
		quotaElem.RestoreAllocation(elem)

		return
	}

	if elem.Kind == database.DumpElemKindQuotaConfig {
		quotaElem := s.getOrInitQuotaElem(elem.Key, 0, elem.Ownership)
		quotaElem.RestoreConfig(elem)

		return
	}

	fqElem := NewFqElem(elem.BatchSize)
	fqElem.ver = elem.Tx
	fqElem.lastTxAt = elem.TxAt
	fqElem.value = elem.Value

	key := hashTableKey{key: elem.Key, batchSize: elem.BatchSize}

	s.mu.Lock()
	s.m[key] = fqElem
	s.mu.Unlock()
}

func (s *HashTable) getOrInitElem(key hashTableKey) *FqElem {
	// Fast path: try read lock first
	s.mu.RLock()
	v, ok := s.m[key]
	s.mu.RUnlock()

	if ok {
		return v
	}

	// Slow path: need to create, use write lock
	s.mu.Lock()
	// Double-check after acquiring write lock
	v, ok = s.m[key]
	if !ok {
		v = NewFqElem(key.batchSize)
		s.m[key] = v
	}
	s.mu.Unlock()

	return v
}

func (s *HashTable) getOrInitSlidingWindowElem(key hashTableKey) *SlidingWindowElem {
	s.mu.RLock()
	v, ok := s.sw[key]
	s.mu.RUnlock()

	if ok {
		return v
	}

	s.mu.Lock()
	v, ok = s.sw[key]
	if !ok {
		v = NewSlidingWindowElem(key.batchSize)
		s.sw[key] = v
	}
	s.mu.Unlock()

	return v
}

func (s *HashTable) getOrInitTokenBucketElem(key hashTableKey) *TokenBucketElem {
	s.mu.RLock()
	v, ok := s.tb[key]
	s.mu.RUnlock()

	if ok {
		return v
	}

	s.mu.Lock()
	v, ok = s.tb[key]
	if !ok {
		v = NewTokenBucketElem(key.batchSize)
		s.tb[key] = v
	}
	s.mu.Unlock()

	return v
}

func (s *HashTable) getOrInitQuotaElem(
	name string,
	limit database.ValueType,
	ownership database.QuotaOwnership,
) *QuotaElem {
	s.mu.RLock()
	v, ok := s.q[name]
	s.mu.RUnlock()

	if ok {
		return v
	}

	s.mu.Lock()
	v, ok = s.q[name]
	if !ok {
		v = NewQuotaElem(limit, ownership)
		s.q[name] = v
	}
	s.mu.Unlock()

	return v
}
