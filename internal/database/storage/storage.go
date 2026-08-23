package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"fq/internal/database"
	"fq/internal/tools"
)

type Engine interface {
	Incr(database.TxContext, database.BatchKey) database.ValueType
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
	Get(database.BatchKey) (database.ValueType, bool)
	Del(database.TxContext, database.BatchKey) bool
	MDel(database.TxContext, []database.BatchKey) []bool
	Clean(context.Context)
	Dump(context.Context, database.Tx) (<-chan database.DumpElem, <-chan error)
	RestoreDumpElem(context.Context, database.DumpElem) error
}

type WAL interface {
	Start()
	Shutdown()
	Incr(ctx context.Context, txCtx database.TxContext, key database.BatchKey) tools.FutureError
	Del(ctx context.Context, txCtx database.TxContext, key database.BatchKey) tools.FutureError
	MDel(ctx context.Context, txCtx database.TxContext, keys []database.BatchKey) tools.FutureError
	RLimitSlidingWindow(ctx context.Context, txCtx database.TxContext, key database.BatchKey) tools.FutureError
	RLimitTokenBucket(
		ctx context.Context,
		txCtx database.TxContext,
		key database.BatchKey,
		capacity database.ValueType,
		refillAmount database.ValueType,
	) tools.FutureError
	TryRecoverWALSegments(ctx context.Context, dumpLastLSN uint64) (lastLSN uint64, err error)
}

type Dumper interface {
	Dump(ctx context.Context, dumpTx database.Tx) error
}

type Replica interface {
	Start(context.Context)
	IsMaster() bool
	Shutdown()
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
	limitEvents             map[chan database.LimitEvent]limitEventSubscriber
	limitEventQueueCapacity int
	eventsMu                sync.RWMutex
}

type limitEventSubscriber struct {
	prefix string
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

	return &Storage{
		engine:                  engine,
		wal:                     wal,
		dumper:                  dumper,
		replica:                 replica,
		logger:                  logger,
		cleanInterval:           cleanInterval,
		dumpInterval:            dumpInterval,
		syncCommit:              syncCommit,
		limitEventQueueCapacity: limitEventQueueCapacity,
		limitEvents:             make(map[chan database.LimitEvent]limitEventSubscriber),
	}, nil
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
		go s.dumpLoop(ctx)
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
	txCtx := s.makeTxContext()

	if s.wal != nil {
		future := s.wal.Incr(ctx, txCtx, key)
		if s.syncCommit {
			if err := future.Get(); err != nil {
				return 0, err
			}
		}
	}

	return s.engine.Incr(txCtx, key), nil
}

func (s *Storage) RLimitFixedWindow(
	ctx context.Context,
	key database.BatchKey,
	limit database.ValueType,
) (database.RateLimitResult, error) {
	txCtx := s.makeTxContext()

	result, err := s.engine.RLimitFixedWindow(txCtx, key, limit, func() error {
		if s.wal == nil {
			return nil
		}

		future := s.wal.Incr(ctx, txCtx, key)
		if s.syncCommit {
			return future.Get()
		}

		return nil
	})
	if err != nil {
		return database.RateLimitResult{}, err
	}

	s.publishLimitFilled(key, result)

	return result, nil
}

func (s *Storage) RLimitSlidingWindow(
	ctx context.Context,
	key database.BatchKey,
	limit database.ValueType,
) (database.RateLimitResult, error) {
	txCtx := s.makeTxContext()

	result, err := s.engine.RLimitSlidingWindow(txCtx, key, limit, func() error {
		if s.wal == nil {
			return nil
		}

		future := s.wal.RLimitSlidingWindow(ctx, txCtx, key)
		if s.syncCommit {
			return future.Get()
		}

		return nil
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
	txCtx := s.makeTxContext()

	result, err := s.engine.RLimitTokenBucket(txCtx, key, capacity, refillAmount, func() error {
		if s.wal == nil {
			return nil
		}

		future := s.wal.RLimitTokenBucket(ctx, txCtx, key, capacity, refillAmount)
		if s.syncCommit {
			return future.Get()
		}

		return nil
	})
	if err != nil {
		return database.RateLimitResult{}, err
	}

	s.publishLimitFilled(key, result)

	return result, nil
}

func (s *Storage) Get(_ context.Context, key database.BatchKey) (database.ValueType, error) {
	value, _ := s.engine.Get(key)

	return value, nil
}

func (s *Storage) Del(ctx context.Context, key database.BatchKey) (bool, error) {
	txCtx := s.makeTxContext()

	if s.wal != nil {
		future := s.wal.Del(ctx, txCtx, key)
		if s.syncCommit {
			if err := future.Get(); err != nil {
				return false, err
			}
		}
	}

	return s.engine.Del(txCtx, key), nil
}

func (s *Storage) MDel(ctx context.Context, keys []database.BatchKey) ([]bool, error) {
	txCtx := s.makeTxContext()

	if s.wal != nil {
		future := s.wal.MDel(ctx, txCtx, keys)
		if s.syncCommit {
			if err := future.Get(); err != nil {
				return nil, err
			}
		}
	}

	return s.engine.MDel(txCtx, keys), nil
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

//nolint:gocritic // ok
func (s *Storage) SubscribeLimitEvents(ctx context.Context, prefix string) (<-chan database.LimitEvent, func()) {
	ch := make(chan database.LimitEvent, s.limitEventQueueCapacity)

	s.eventsMu.Lock()
	s.limitEvents[ch] = limitEventSubscriber{prefix: prefix}
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

	s.eventsMu.RLock()
	defer s.eventsMu.RUnlock()

	for ch, subscriber := range s.limitEvents {
		if subscriber.prefix != "" && !strings.HasPrefix(event.Key, subscriber.prefix) {
			continue
		}

		select {
		case ch <- event:
		default:
			s.logger.Warn().
				Str("key", event.Key).
				Uint32("window", event.Window).
				Msg("limit event subscriber is slow, dropping event")
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
