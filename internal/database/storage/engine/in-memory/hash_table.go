package inmemory

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/google/btree"

	"github.com/fq-db/fq/internal/database"
)

var HashTableBuilder = func() hashTable {
	return NewHashTable()
}

var IndexedHashTableBuilder = func() hashTable {
	return NewIndexedHashTable()
}

const (
	cleanChunkSize       = 1024
	cleanChunkTimeBudget = 2 * time.Millisecond
	keyIndexDegree       = 32

	snapshotCancelCheckStride = 4096

	indexCompactStaleThreshold      = 10_000
	indexCompactSmallStaleThreshold = 1_000
	indexCompactSmallLiveThreshold  = 10_000
	indexCompactLiveStaleRatio      = 4
	indexCompactScanChunkSize       = 512
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

	index              *btree.BTreeG[hashTableKey]
	indexStaleCount    uint64
	indexCompactAfter  hashTableKey
	indexCompactActive bool
}

func NewHashTable() *HashTable {
	return &HashTable{
		m:  make(map[hashTableKey]*FqElem),
		sw: make(map[hashTableKey]*SlidingWindowElem),
		tb: make(map[hashTableKey]*TokenBucketElem),
		q:  make(map[string]*QuotaElem),
	}
}

func NewIndexedHashTable() *HashTable {
	table := NewHashTable()
	table.index = btree.NewG[hashTableKey](keyIndexDegree, lessHashTableKey)

	return table
}

func (s *HashTable) Incr(
	txCtx database.TxContext,
	key database.BatchKey,
	beforeApply func() error,
) (database.ValueType, error) {
	htKey := hashTableKey{key: key.Key, batchSize: key.BatchSize}
	v := s.getOrInitElem(htKey)

	return v.Incr(txCtx, beforeApply)
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
	request database.QuotaSetRequest,
	beforeApply func() error,
) (bool, error) {
	v := s.getOrInitQuotaElem(request.Name, 0, database.QuotaOwnershipServer)

	return v.SetConfig(txCtx, request, beforeApply)
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
	s.removeIndexKeyIfDeadLocked(htKey)
	s.mu.Unlock()

	return counterFound || slidingWindowFound || tokenBucketFound
}

func (s *HashTable) Stats() database.PartitionStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	allocations := 0
	for _, q := range s.q {
		allocations += q.AllocationCount()
	}

	return database.PartitionStats{
		Counters:         len(s.m),
		SlidingWindows:   len(s.sw),
		TokenBuckets:     len(s.tb),
		Quotas:           len(s.q),
		QuotaAllocations: allocations,
	}
}

func (s *HashTable) FlushDB() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.m = make(map[hashTableKey]*FqElem)
	s.sw = make(map[hashTableKey]*SlidingWindowElem)
	s.tb = make(map[hashTableKey]*TokenBucketElem)
	s.q = make(map[string]*QuotaElem)
	if s.index != nil {
		s.index = btree.NewG[hashTableKey](keyIndexDegree, lessHashTableKey)
	}
	s.indexStaleCount = 0
	s.resetIndexCompactStateLocked()
}

func (s *HashTable) Scan(prefix string, after hashTableKey, count uint32) []database.BatchKey {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if count == 0 || s.index == nil {
		return nil
	}

	res := make([]database.BatchKey, 0, count)
	s.index.AscendGreaterOrEqual(after, func(item hashTableKey) bool {
		if !lessHashTableKey(after, item) {
			return true
		}
		if prefix != "" && !strings.HasPrefix(item.key, prefix) {
			return item.key < prefix
		}
		if !s.keyExistsLocked(item) {
			return true
		}

		res = append(res, database.BatchKey{
			Key:          item.key,
			BatchSize:    item.batchSize,
			BatchSizeStr: fmt.Sprintf("%d", item.batchSize),
		})

		return len(res) < int(count)
	})

	return res
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
				s.markIndexStaleLocked(item.key)
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
				s.markIndexStaleLocked(item.key)
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

func (s *HashTable) Snapshot(ctx context.Context, dumpTx database.Tx) []database.DumpElem {
	s.mu.RLock()
	defer s.mu.RUnlock()

	elems := make([]database.DumpElem, 0, len(s.m)+len(s.sw)+len(s.tb)+len(s.q))
	checked := 0
	canceled := func() bool {
		checked++
		if checked%snapshotCancelCheckStride != 0 {
			return false
		}

		return ctx.Err() != nil
	}

	for key, elem := range s.m {
		if canceled() {
			return elems
		}

		value, txAt, tx := elem.DumpValue(dumpTx)
		if value == database.ErrorValue {
			continue
		}

		elems = append(elems, database.DumpElem{
			Kind:      database.DumpElemKindCounter,
			Key:       key.key,
			BatchSize: key.batchSize,
			Value:     value,
			TxAt:      txAt,
			Tx:        tx,
		})
	}

	for key, elem := range s.sw {
		if canceled() {
			return elems
		}

		elems = elem.AppendDump(elems, key, dumpTx)
	}

	for key, elem := range s.tb {
		if canceled() {
			return elems
		}

		value, txAt, tx := elem.DumpValue(dumpTx)
		if value == database.ErrorValue {
			continue
		}

		elems = append(elems, database.DumpElem{
			Kind:      database.DumpElemKindTokenBucket,
			Key:       key.key,
			BatchSize: key.batchSize,
			Value:     value,
			TxAt:      txAt,
			Tx:        tx,
		})
	}

	now := database.TxTime(time.Now().Unix())
	for key, elem := range s.q {
		if canceled() {
			return elems
		}

		if config := elem.dumpConfig(dumpTx); config.ok {
			elems = append(elems, database.DumpElem{
				Kind:      database.DumpElemKindQuotaConfig,
				Key:       key,
				Limit:     config.limit,
				Ownership: config.ownership,
				Policy:    config.policy,
				Clients:   config.clients,
				Tx:        config.tx,
			})
		}

		limit := elem.Limit()
		for _, allocation := range elem.dumpAllocations(dumpTx, now) {
			elems = append(elems, database.DumpElem{
				Kind:      database.DumpElemKindQuotaAllocation,
				Key:       key,
				Limit:     limit,
				Value:     allocation.amount,
				Ownership: elem.Ownership(),
				Policy:    elem.Policy(),
				Clients:   elem.Clients(),
				ClientID:  allocation.clientID,
				ExpiresAt: allocation.expiresAt,
				TxAt:      allocation.txAt,
				Tx:        allocation.tx,
			})
		}
	}

	return elems
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
	s.indexKeyLocked(key)
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
		s.indexKeyLocked(key)
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
		s.indexKeyLocked(key)
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
		s.indexKeyLocked(key)
	}
	s.mu.Unlock()

	return v
}

func (s *HashTable) CompactIndex(ctx context.Context, maxDeletes int, budget time.Duration) bool {
	if maxDeletes <= 0 {
		return false
	}

	s.mu.RLock()
	needed := s.shouldCompactIndexLocked()
	s.mu.RUnlock()

	if !needed {
		return true
	}

	startedAt := time.Now()
	deleted := 0

	for deleted < maxDeletes {
		if ctx.Err() != nil {
			return false
		}

		reachedEnd, chunkDeleted := s.compactIndexChunk(maxDeletes - deleted)
		deleted += chunkDeleted
		if reachedEnd {
			return true
		}

		if time.Since(startedAt) >= budget {
			return false
		}

		runtime.Gosched()
	}

	return false
}

func (s *HashTable) compactIndexChunk(maxDeletes int) (reachedEnd bool, deleted int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.index == nil {
		s.resetIndexCompactStateLocked()

		return true, 0
	}

	pivot := s.indexCompactAfter
	resumed := s.indexCompactActive
	stale := make([]hashTableKey, 0, maxDeletes)
	scanned := 0
	reachedEnd = true

	iter := func(item hashTableKey) bool {
		if resumed && !lessHashTableKey(pivot, item) {
			return true
		}

		s.indexCompactAfter = item
		s.indexCompactActive = true
		scanned++

		if !s.keyExistsLocked(item) {
			stale = append(stale, item)
		}

		if len(stale) >= maxDeletes || scanned >= indexCompactScanChunkSize {
			reachedEnd = false

			return false
		}

		return true
	}

	if resumed {
		s.index.AscendGreaterOrEqual(pivot, iter)
	} else {
		s.index.Ascend(iter)
	}

	for _, key := range stale {
		s.index.Delete(key)
		if s.indexStaleCount > 0 {
			s.indexStaleCount--
		}
	}

	if reachedEnd {
		s.indexStaleCount = 0
		s.resetIndexCompactStateLocked()
	}

	return reachedEnd, len(stale)
}

func (s *HashTable) shouldCompactIndexLocked() bool {
	if s.index == nil || s.indexStaleCount == 0 {
		return false
	}
	if s.indexCompactActive {
		return true
	}

	live := s.liveKeyCountLocked()
	if s.indexStaleCount >= indexCompactStaleThreshold &&
		s.indexStaleCount >= live/indexCompactLiveStaleRatio {
		return true
	}

	return s.indexStaleCount >= indexCompactSmallStaleThreshold && live < indexCompactSmallLiveThreshold
}

func (s *HashTable) liveKeyCountLocked() uint64 {
	return uint64(len(s.m)) + uint64(len(s.sw)) + uint64(len(s.tb))
}

func (s *HashTable) resetIndexCompactStateLocked() {
	s.indexCompactAfter = hashTableKey{}
	s.indexCompactActive = false
}

func (s *HashTable) markIndexStaleLocked(key hashTableKey) {
	if s.index == nil || s.keyExistsLocked(key) {
		return
	}
	s.indexStaleCount++
}

func (s *HashTable) indexKeyLocked(key hashTableKey) {
	if s.index == nil {
		return
	}
	s.index.ReplaceOrInsert(key)
}

func (s *HashTable) removeIndexKeyIfDeadLocked(key hashTableKey) {
	if s.index == nil || s.keyExistsLocked(key) {
		return
	}
	s.index.Delete(key)
}

func (s *HashTable) keyExistsLocked(key hashTableKey) bool {
	if _, ok := s.m[key]; ok {
		return true
	}
	if _, ok := s.sw[key]; ok {
		return true
	}
	if _, ok := s.tb[key]; ok {
		return true
	}

	return false
}

func lessHashTableKey(a, b hashTableKey) bool {
	if a.key != b.key {
		return a.key < b.key
	}

	return a.batchSize < b.batchSize
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
