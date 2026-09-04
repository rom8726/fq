package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/tools"
)

type Engine interface {
	Incr(database.TxContext, database.BatchKey, func() error) (database.ValueType, error)
	RLimitFixedWindow(
		database.TxContext,
		database.BatchKey,
		database.ValueType,
		func() error,
	) (database.RateLimitResult, error)
	RLimitSlidingWindow(
		database.TxContext,
		database.BatchKey,
		database.ValueType,
		func() error,
	) (database.RateLimitResult, error)
	RLimitTokenBucket(
		database.TxContext,
		database.BatchKey,
		database.ValueType,
		database.ValueType,
		func() error,
	) (database.RateLimitResult, error)
	QuotaAcquire(
		database.TxContext,
		database.QuotaAcquireRequest,
		func() error,
	) (database.QuotaAcquireResult, error)
	QuotaSet(database.TxContext, database.QuotaSetRequest, func() error) (bool, error)
	QuotaRelease(database.TxContext, string, string, func() error) (database.QuotaReleaseResult, error)
	QuotaDelete(database.TxContext, string, func() error) (bool, error)
	QuotaInfo(database.TxTime, string) database.QuotaInfo
	Get(database.BatchKey) (database.ValueType, bool)
	Del(database.TxContext, database.BatchKey) bool
	MDel(database.TxContext, []database.BatchKey) []bool
	FlushDB()
	Stats() database.EngineStats
	Scan(prefix, cursor string, count uint32) (database.ScanResult, error)
	Clean(context.Context)
	Snapshot(context.Context, database.Tx) (database.DumpSnapshot, error)
	RestoreDumpElem(context.Context, database.DumpElem) error
}

type WAL interface {
	Start()
	Shutdown()
	Incr(ctx context.Context, txCtx database.TxContext, key database.BatchKey) tools.FutureError
	IncrAsync(ctx context.Context, txCtx database.TxContext, key database.BatchKey)
	Del(ctx context.Context, txCtx database.TxContext, key database.BatchKey) tools.FutureError
	DelAsync(ctx context.Context, txCtx database.TxContext, key database.BatchKey)
	MDel(ctx context.Context, txCtx database.TxContext, keys []database.BatchKey) tools.FutureError
	MDelAsync(ctx context.Context, txCtx database.TxContext, keys []database.BatchKey)
	RLimitSlidingWindow(
		ctx context.Context,
		txCtx database.TxContext,
		key database.BatchKey,
		limit database.ValueType,
	) tools.FutureError
	RLimitSlidingWindowAsync(
		ctx context.Context,
		txCtx database.TxContext,
		key database.BatchKey,
		limit database.ValueType,
	)
	RLimitTokenBucket(
		ctx context.Context,
		txCtx database.TxContext,
		key database.BatchKey,
		capacity database.ValueType,
		refillAmount database.ValueType,
	) tools.FutureError
	RLimitFixedWindow(
		ctx context.Context,
		txCtx database.TxContext,
		key database.BatchKey,
		limit database.ValueType,
	) tools.FutureError
	RLimitFixedWindowAsync(ctx context.Context, txCtx database.TxContext, key database.BatchKey, limit database.ValueType)
	RLimitTokenBucketAsync(
		ctx context.Context,
		txCtx database.TxContext,
		key database.BatchKey,
		capacity database.ValueType,
		refillAmount database.ValueType,
	)
	QuotaAcquire(ctx context.Context, txCtx database.TxContext, request database.QuotaAcquireRequest) tools.FutureError
	QuotaAcquireAsync(ctx context.Context, txCtx database.TxContext, request database.QuotaAcquireRequest)
	QuotaSet(ctx context.Context, txCtx database.TxContext, request database.QuotaSetRequest) tools.FutureError
	QuotaSetAsync(ctx context.Context, txCtx database.TxContext, request database.QuotaSetRequest)
	QuotaRelease(ctx context.Context, txCtx database.TxContext, name string, clientID string) tools.FutureError
	QuotaReleaseAsync(ctx context.Context, txCtx database.TxContext, name string, clientID string)
	QuotaDelete(ctx context.Context, txCtx database.TxContext, name string) tools.FutureError
	QuotaDeleteAsync(ctx context.Context, txCtx database.TxContext, name string)
	FlushDB(ctx context.Context, txCtx database.TxContext) tools.FutureError
	Truncate(ctx context.Context, txCtx database.TxContext) tools.FutureError
	TryRecoverWALSegments(ctx context.Context, dumpLastLSN uint64) (lastLSN uint64, err error)
}

type Dumper interface {
	Dump(ctx context.Context, dumpTx database.Tx, snapshot database.DumpSnapshot) error
	Truncate(ctx context.Context) error
}

type Replica interface {
	Start(context.Context)
	IsMaster() bool
	Shutdown()
}

type walRecoveryObserver interface {
	SetRecoveredWALState(lastAppliedLSN uint64)
}

type quotaEventPublisher interface {
	SetQuotaEventPublisher(func(database.QuotaEvent))
}

type limitEventPublisher interface {
	SetLimitEventPublisher(func(database.LimitEvent))
}

var ErrInvalidLimitEventQueueCapacity = errors.New("limit event queue capacity is invalid")

type Storage struct {
	engine        Engine
	wal           WAL
	dumper        Dumper
	replica       Replica
	logger        *zerolog.Logger
	cleanInterval time.Duration
	dumpInterval  time.Duration
	syncCommit    bool

	tx                      atomic.Uint64
	dumpTx                  atomic.Uint64
	mutationMu              sync.RWMutex
	dumpOpMu                sync.Mutex
	limitEvents             map[chan database.LimitEvent]*limitEventSubscriber
	quotaEvents             map[chan database.QuotaEvent]*quotaEventSubscriber
	limitEventQueueCapacity int
	eventsMu                sync.RWMutex

	lastDump atomic.Pointer[dumpSnapshot]
}

type dumpSnapshot struct {
	at       time.Time
	duration time.Duration
	err      error
	tx       database.Tx
}

type limitEventSubscriber struct {
	prefix  string
	dropped atomic.Uint64
}

type quotaEventSubscriber struct {
	prefix  string
	dropped atomic.Uint64
}

func NewStorage(
	engine Engine,
	wal WAL,
	dumper Dumper,
	replica Replica,
	logger *zerolog.Logger,
	cleanInterval time.Duration,
	dumpInterval time.Duration,
	syncCommit bool,
	limitEventQueueCapacity int,
) (*Storage, error) {
	if engine == nil {
		return nil, errors.New("engine is invalid")
	}

	if logger == nil {
		return nil, errors.New("logger is invalid")
	}

	if limitEventQueueCapacity <= 0 {
		return nil, ErrInvalidLimitEventQueueCapacity
	}

	storage := &Storage{
		engine:                  engine,
		wal:                     wal,
		dumper:                  dumper,
		replica:                 replica,
		logger:                  logger,
		cleanInterval:           cleanInterval,
		dumpInterval:            dumpInterval,
		syncCommit:              syncCommit,
		limitEventQueueCapacity: limitEventQueueCapacity,
		limitEvents:             make(map[chan database.LimitEvent]*limitEventSubscriber),
		quotaEvents:             make(map[chan database.QuotaEvent]*quotaEventSubscriber),
	}

	if publisher, ok := engine.(quotaEventPublisher); ok {
		publisher.SetQuotaEventPublisher(storage.publishQuotaEvent)
	}
	if publisher, ok := engine.(limitEventPublisher); ok {
		publisher.SetLimitEventPublisher(storage.publishLimitEvent)
	}

	return storage, nil
}

func (s *Storage) LoadWAL(ctx context.Context, dumpLastTx database.Tx) error {
	if s.wal == nil {
		s.tx.Store(uint64(dumpLastTx))

		return nil
	}

	lastLSN, err := s.wal.TryRecoverWALSegments(ctx, uint64(dumpLastTx))
	if err != nil {
		return err
	}

	if uint64(dumpLastTx) > lastLSN {
		lastLSN = uint64(dumpLastTx)
	}

	s.tx.Store(lastLSN)
	if s.replica != nil && !s.replica.IsMaster() {
		if observer, ok := s.replica.(walRecoveryObserver); ok {
			observer.SetRecoveredWALState(lastLSN)
		}
	}

	return nil
}

func (s *Storage) Start(ctx context.Context) {
	if s.wal != nil {
		if s.replica != nil {
			if s.replica.IsMaster() {
				s.wal.Start()
			}

			s.replica.Start(ctx)
		} else {
			s.wal.Start()
		}
	}

	go s.gcLoop(ctx)
	if s.dumper != nil {
		if s.replica == nil || s.replica.IsMaster() {
			go s.dumpLoop(ctx)
		} else {
			s.logger.Info().Msg("periodic dump is disabled on a replica")
		}
	}
}

func (s *Storage) Shutdown() {
	shutdownTimeout := 30 * time.Second
	shutdownDone := make(chan struct{})

	go func() {
		defer close(shutdownDone)

		// Shutdown replica first (slave needs to stop before channels are closed)
		if s.replica != nil {
			s.replica.Shutdown()
		}

		if s.wal != nil {
			if s.replica == nil || s.replica.IsMaster() {
				s.wal.Shutdown()
			}
		}

		// Shutdown dumper if it has shutdown method
		if dumperWithShutdown, ok := s.dumper.(interface{ Shutdown() }); ok {
			dumperWithShutdown.Shutdown()
		}
	}()

	select {
	case <-shutdownDone:
		s.logger.Info().Msg("storage shutdown completed")
	case <-time.After(shutdownTimeout):
		s.logger.Warn().Msg("storage shutdown timeout exceeded")
	}
}

func (s *Storage) Incr(ctx context.Context, key database.BatchKey) (database.ValueType, error) {
	s.mutationMu.RLock()
	defer s.mutationMu.RUnlock()

	txCtx := s.makeTxContext()

	return s.engine.Incr(txCtx, key, func() error {
		return s.writeIncrWAL(ctx, txCtx, key)
	})
}

//nolint:dupl // ok
func (s *Storage) RLimitFixedWindow(
	ctx context.Context,
	key database.BatchKey,
	limit database.ValueType,
) (database.RateLimitResult, error) {
	s.mutationMu.RLock()
	defer s.mutationMu.RUnlock()

	txCtx := s.makeTxContext()

	result, err := s.engine.RLimitFixedWindow(txCtx, key, limit, func() error {
		return s.writeRLimitFixedWindowWAL(ctx, txCtx, key, limit)
	})
	if err != nil {
		return database.RateLimitResult{}, err
	}

	s.publishLimitFilled(key, result)

	return result, nil
}

//nolint:dupl // ok
func (s *Storage) RLimitSlidingWindow(
	ctx context.Context,
	key database.BatchKey,
	limit database.ValueType,
) (database.RateLimitResult, error) {
	s.mutationMu.RLock()
	defer s.mutationMu.RUnlock()

	txCtx := s.makeTxContext()

	result, err := s.engine.RLimitSlidingWindow(txCtx, key, limit, func() error {
		return s.writeRLimitSlidingWindowWAL(ctx, txCtx, key, limit)
	})
	if err != nil {
		return database.RateLimitResult{}, err
	}

	s.publishLimitFilled(key, result)

	return result, nil
}

func (s *Storage) RLimitTokenBucket(
	ctx context.Context,
	key database.BatchKey,
	capacity database.ValueType,
	refillAmount database.ValueType,
) (database.RateLimitResult, error) {
	s.mutationMu.RLock()
	defer s.mutationMu.RUnlock()

	txCtx := s.makeTxContext()

	result, err := s.engine.RLimitTokenBucket(txCtx, key, capacity, refillAmount, func() error {
		return s.writeRLimitTokenBucketWAL(ctx, txCtx, key, capacity, refillAmount)
	})
	if err != nil {
		return database.RateLimitResult{}, err
	}

	s.publishLimitFilled(key, result)

	return result, nil
}

func (s *Storage) QuotaAcquire(
	ctx context.Context,
	request database.QuotaAcquireRequest,
) (database.QuotaAcquireResult, error) {
	s.mutationMu.RLock()
	defer s.mutationMu.RUnlock()

	txCtx := s.makeTxContext()
	if request.TTL > 0 {
		request.ExpiresAt = txCtx.CurrTime + database.TxTime(request.TTL)
	}

	result, err := s.engine.QuotaAcquire(txCtx, request, func() error {
		return s.writeQuotaAcquireWAL(ctx, txCtx, request)
	})
	if err != nil {
		return database.QuotaAcquireResult{}, err
	}
	if result.Mutated {
		s.publishQuotaEvent(database.QuotaEvent{
			Event:     "acq",
			Name:      request.Name,
			ClientID:  request.ClientID,
			Amount:    result.Allocated,
			Used:      result.Used,
			Remaining: result.Remaining,
			ExpiresAt: request.ExpiresAt,
		})
	}

	return result, nil
}

func (s *Storage) QuotaSet(ctx context.Context, request database.QuotaSetRequest) (bool, error) {
	s.mutationMu.RLock()
	defer s.mutationMu.RUnlock()

	txCtx := s.makeTxContext()

	changed, err := s.engine.QuotaSet(txCtx, request, func() error {
		return s.writeQuotaSetWAL(ctx, txCtx, request)
	})
	if err != nil {
		return false, err
	}

	return changed, nil
}

func (s *Storage) QuotaRelease(ctx context.Context, name, clientID string) (bool, error) {
	s.mutationMu.RLock()
	defer s.mutationMu.RUnlock()

	txCtx := s.makeTxContext()

	result, err := s.engine.QuotaRelease(txCtx, name, clientID, func() error {
		return s.writeQuotaReleaseWAL(ctx, txCtx, name, clientID)
	})
	if err != nil {
		return false, err
	}
	if result.Released {
		s.publishQuotaEvent(database.QuotaEvent{
			Event:     "rel",
			Name:      name,
			ClientID:  clientID,
			Amount:    result.Amount,
			Used:      result.Used,
			Remaining: result.Remaining,
			ExpiresAt: result.ExpiresAt,
		})
	}

	return result.Released, nil
}

func (s *Storage) QuotaDelete(ctx context.Context, name string) (bool, error) {
	s.mutationMu.RLock()
	defer s.mutationMu.RUnlock()

	txCtx := s.makeTxContext()

	deleted, err := s.engine.QuotaDelete(txCtx, name, func() error {
		return s.writeQuotaDeleteWAL(ctx, txCtx, name)
	})
	if err != nil {
		return false, err
	}
	if deleted {
		s.publishQuotaEvent(database.QuotaEvent{
			Event: "del",
			Name:  name,
		})
	}

	return deleted, nil
}

func (s *Storage) QuotaInfo(_ context.Context, name string) (database.QuotaInfo, error) {
	now := database.TxTime(time.Now().Unix())

	return s.engine.QuotaInfo(now, name), nil
}

func (s *Storage) Get(_ context.Context, key database.BatchKey) (database.ValueType, error) {
	value, _ := s.engine.Get(key)

	return value, nil
}

func (s *Storage) Del(ctx context.Context, key database.BatchKey) (bool, error) {
	s.mutationMu.RLock()
	defer s.mutationMu.RUnlock()

	txCtx := s.makeTxContext()

	if err := s.writeDelWAL(ctx, txCtx, key); err != nil {
		return false, err
	}

	return s.engine.Del(txCtx, key), nil
}

func (s *Storage) MDel(ctx context.Context, keys []database.BatchKey) ([]bool, error) {
	s.mutationMu.RLock()
	defer s.mutationMu.RUnlock()

	txCtx := s.makeTxContext()

	if err := s.writeMDelWAL(ctx, txCtx, keys); err != nil {
		return nil, err
	}

	return s.engine.MDel(txCtx, keys), nil
}

func (s *Storage) FlushDB(ctx context.Context) error {
	s.dumpOpMu.Lock()
	defer s.dumpOpMu.Unlock()

	if s.dumper != nil {
		if err := s.dumper.Truncate(ctx); err != nil {
			return err
		}
	}

	s.mutationMu.RLock()
	defer s.mutationMu.RUnlock()

	txCtx := s.makeTxContext()
	if s.wal != nil {
		future := s.wal.FlushDB(ctx, txCtx)
		if err := future.Get(); err != nil {
			return err
		}
	}

	s.engine.FlushDB()

	return nil
}

func (s *Storage) Truncate(ctx context.Context) error {
	s.dumpOpMu.Lock()
	defer s.dumpOpMu.Unlock()

	if s.dumper != nil {
		if err := s.dumper.Truncate(ctx); err != nil {
			return err
		}
	}

	s.mutationMu.RLock()
	defer s.mutationMu.RUnlock()

	txCtx := s.makeTxContext()
	if s.wal != nil {
		future := s.wal.Truncate(ctx, txCtx)
		if err := future.Get(); err != nil {
			return err
		}
	}

	s.engine.FlushDB()
	s.dumpTx.Store(0)

	return nil
}

func (s *Storage) Scan(_ context.Context, prefix, cursor string, count uint32) (database.ScanResult, error) {
	return s.engine.Scan(prefix, cursor, count)
}

func (s *Storage) EngineStats() database.EngineStats {
	return s.engine.Stats()
}

func (s *Storage) CurrentLSN() uint64 {
	return s.tx.Load()
}

func (s *Storage) DumpLSN() uint64 {
	return s.dumpTx.Load()
}

func (s *Storage) SyncCommit() bool {
	return s.syncCommit
}

var ErrDumpNeverRun = errors.New("dump has never run")

func (s *Storage) LastDump() (at time.Time, duration time.Duration, tx database.Tx, err error) {
	snap := s.lastDump.Load()
	if snap == nil {
		return time.Time{}, 0, 0, ErrDumpNeverRun
	}

	return snap.at, snap.duration, snap.tx, snap.err
}

type SubscriberStat struct {
	Prefix    string
	HasPrefix bool
	QueueLen  int
	QueueCap  int
	Dropped   uint64
}

type StreamStats struct {
	LimitSubscribers []SubscriberStat
	QuotaSubscribers []SubscriberStat
}

func (s *Storage) StreamStats() StreamStats {
	s.eventsMu.RLock()
	defer s.eventsMu.RUnlock()

	stats := StreamStats{
		LimitSubscribers: make([]SubscriberStat, 0, len(s.limitEvents)),
		QuotaSubscribers: make([]SubscriberStat, 0, len(s.quotaEvents)),
	}

	for ch, sub := range s.limitEvents {
		stats.LimitSubscribers = append(stats.LimitSubscribers, SubscriberStat{
			Prefix:    sub.prefix,
			HasPrefix: sub.prefix != "",
			QueueLen:  len(ch),
			QueueCap:  cap(ch),
			Dropped:   sub.dropped.Load(),
		})
	}

	for ch, sub := range s.quotaEvents {
		stats.QuotaSubscribers = append(stats.QuotaSubscribers, SubscriberStat{
			Prefix:    sub.prefix,
			HasPrefix: sub.prefix != "",
			QueueLen:  len(ch),
			QueueCap:  cap(ch),
			Dropped:   sub.dropped.Load(),
		})
	}

	return stats
}

func (s *Storage) writeIncrWAL(ctx context.Context, txCtx database.TxContext, key database.BatchKey) error {
	if s.wal == nil {
		return nil
	}

	if s.syncCommit {
		future := s.wal.Incr(ctx, txCtx, key)
		return future.Get()
	}

	s.wal.IncrAsync(ctx, txCtx, key)

	return nil
}

func (s *Storage) writeDelWAL(ctx context.Context, txCtx database.TxContext, key database.BatchKey) error {
	if s.wal == nil {
		return nil
	}

	if s.syncCommit {
		future := s.wal.Del(ctx, txCtx, key)
		return future.Get()
	}

	s.wal.DelAsync(ctx, txCtx, key)

	return nil
}

func (s *Storage) writeMDelWAL(ctx context.Context, txCtx database.TxContext, keys []database.BatchKey) error {
	if s.wal == nil {
		return nil
	}

	if s.syncCommit {
		future := s.wal.MDel(ctx, txCtx, keys)
		return future.Get()
	}

	s.wal.MDelAsync(ctx, txCtx, keys)

	return nil
}

func (s *Storage) writeRLimitSlidingWindowWAL(
	ctx context.Context,
	txCtx database.TxContext,
	key database.BatchKey,
	limit database.ValueType,
) error {
	if s.wal == nil {
		return nil
	}

	if s.syncCommit {
		future := s.wal.RLimitSlidingWindow(ctx, txCtx, key, limit)
		return future.Get()
	}

	s.wal.RLimitSlidingWindowAsync(ctx, txCtx, key, limit)

	return nil
}

func (s *Storage) writeRLimitFixedWindowWAL(
	ctx context.Context,
	txCtx database.TxContext,
	key database.BatchKey,
	limit database.ValueType,
) error {
	if s.wal == nil {
		return nil
	}

	if s.syncCommit {
		future := s.wal.RLimitFixedWindow(ctx, txCtx, key, limit)
		return future.Get()
	}

	s.wal.RLimitFixedWindowAsync(ctx, txCtx, key, limit)

	return nil
}

func (s *Storage) writeRLimitTokenBucketWAL(
	ctx context.Context,
	txCtx database.TxContext,
	key database.BatchKey,
	capacity database.ValueType,
	refillAmount database.ValueType,
) error {
	if s.wal == nil {
		return nil
	}

	if s.syncCommit {
		future := s.wal.RLimitTokenBucket(ctx, txCtx, key, capacity, refillAmount)
		return future.Get()
	}

	s.wal.RLimitTokenBucketAsync(ctx, txCtx, key, capacity, refillAmount)

	return nil
}

func (s *Storage) writeQuotaAcquireWAL(
	ctx context.Context,
	txCtx database.TxContext,
	request database.QuotaAcquireRequest,
) error {
	if s.wal == nil {
		return nil
	}

	if s.syncCommit {
		future := s.wal.QuotaAcquire(ctx, txCtx, request)
		return future.Get()
	}

	s.wal.QuotaAcquireAsync(ctx, txCtx, request)

	return nil
}

func (s *Storage) writeQuotaSetWAL(
	ctx context.Context,
	txCtx database.TxContext,
	request database.QuotaSetRequest,
) error {
	if s.wal == nil {
		return nil
	}

	if s.syncCommit {
		future := s.wal.QuotaSet(ctx, txCtx, request)
		return future.Get()
	}

	s.wal.QuotaSetAsync(ctx, txCtx, request)

	return nil
}

func (s *Storage) writeQuotaReleaseWAL(ctx context.Context, txCtx database.TxContext, name, clientID string) error {
	if s.wal == nil {
		return nil
	}

	if s.syncCommit {
		future := s.wal.QuotaRelease(ctx, txCtx, name, clientID)
		return future.Get()
	}

	s.wal.QuotaReleaseAsync(ctx, txCtx, name, clientID)

	return nil
}

func (s *Storage) writeQuotaDeleteWAL(ctx context.Context, txCtx database.TxContext, name string) error {
	if s.wal == nil {
		return nil
	}

	if s.syncCommit {
		future := s.wal.QuotaDelete(ctx, txCtx, name)
		return future.Get()
	}

	s.wal.QuotaDeleteAsync(ctx, txCtx, name)

	return nil
}

func (s *Storage) Watch(ctx context.Context, key database.BatchKey) (database.ValueType, error) {
	// Get initial value
	lastValue, err := s.Get(ctx, key)
	if err != nil {
		return 0, err
	}

	// Poll for changes every 100ms
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-ticker.C:
			currentValue, err := s.Get(ctx, key)
			if err != nil {
				return 0, err
			}

			// If value changed, return it
			if currentValue != lastValue {
				return currentValue, nil
			}
		}
	}
}

//nolint:gocritic,dupl // ok
func (s *Storage) SubscribeLimitEvents(ctx context.Context, prefix string) (<-chan database.LimitEvent, func()) {
	ch := make(chan database.LimitEvent, s.limitEventQueueCapacity)

	s.eventsMu.Lock()
	s.limitEvents[ch] = &limitEventSubscriber{prefix: prefix}
	s.eventsMu.Unlock()

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			s.eventsMu.Lock()
			delete(s.limitEvents, ch)
			close(ch)
			s.eventsMu.Unlock()
		})
	}

	go func() {
		<-ctx.Done()
		unsubscribe()
	}()

	return ch, unsubscribe
}

//nolint:gocritic,dupl // ok
func (s *Storage) SubscribeQuotaEvents(ctx context.Context, prefix string) (<-chan database.QuotaEvent, func()) {
	ch := make(chan database.QuotaEvent, s.limitEventQueueCapacity)

	s.eventsMu.Lock()
	s.quotaEvents[ch] = &quotaEventSubscriber{prefix: prefix}
	s.eventsMu.Unlock()

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			s.eventsMu.Lock()
			delete(s.quotaEvents, ch)
			close(ch)
			s.eventsMu.Unlock()
		})
	}

	go func() {
		<-ctx.Done()
		unsubscribe()
	}()

	return ch, unsubscribe
}

func (s *Storage) publishLimitFilled(
	key database.BatchKey,
	result database.RateLimitResult,
) {
	if !result.LimitFilled {
		return
	}

	event := database.LimitEvent{
		Key:        key.Key,
		Window:     key.BatchSize,
		Current:    result.Current,
		ResetAfter: result.ResetAfter,
	}

	s.publishLimitEvent(event)
}

func (s *Storage) publishLimitEvent(event database.LimitEvent) {
	s.eventsMu.RLock()
	defer s.eventsMu.RUnlock()

	for ch, subscriber := range s.limitEvents {
		if subscriber.prefix != "" && !strings.HasPrefix(event.Key, subscriber.prefix) {
			continue
		}

		select {
		case ch <- event:
		default:
			subscriber.dropped.Add(1)
			s.logger.Warn().
				Str("key", event.Key).
				Uint32("window", event.Window).
				Msg("limit event subscriber is slow, dropping event")
		}
	}
}

func (s *Storage) publishQuotaEvent(event database.QuotaEvent) {
	s.eventsMu.RLock()
	defer s.eventsMu.RUnlock()

	for ch, subscriber := range s.quotaEvents {
		if subscriber.prefix != "" && !strings.HasPrefix(event.Name, subscriber.prefix) {
			continue
		}

		select {
		case ch <- event:
		default:
			subscriber.dropped.Add(1)
			s.logger.Warn().
				Str("event", event.Event).
				Str("name", event.Name).
				Str("client_id", event.ClientID).
				Msg("quota event subscriber is slow, dropping event")
		}
	}
}

func (s *Storage) makeTxContext() database.TxContext {
	return database.TxContext{
		Tx:       database.Tx(s.tx.Add(1)),
		DumpTx:   database.Tx(s.dumpTx.Load()),
		CurrTime: database.TxTime(time.Now().Unix()),
		FromWAL:  false,
	}
}

func (s *Storage) gcLoop(ctx context.Context) {
	t := time.NewTicker(s.cleanInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.engine.Clean(ctx)
		}
	}
}
