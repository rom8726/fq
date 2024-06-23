package storage

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"fq/internal/database"
	"fq/internal/tools"
)

type Engine interface {
	Incr(database.TxContext, database.BatchKey) database.ValueType
	Get(database.BatchKey) (database.ValueType, bool)
	Del(database.TxContext, database.BatchKey) bool
	Clean(context.Context)
	Dump(context.Context, database.Tx) (<-chan database.DumpElem, <-chan error)
	RestoreDumpElem(context.Context, database.DumpElem) error
}

type WAL interface {
	Start()
	Shutdown()
	Incr(ctx context.Context, txCtx database.TxContext, key database.BatchKey) tools.FutureError
	Del(ctx context.Context, txCtx database.TxContext, key database.BatchKey) tools.FutureError
	TryRecoverWALSegments(ctx context.Context, dumpLastLSN uint64) (lastLSN uint64, err error)
}

type Dumper interface {
	Dump(ctx context.Context, dumpTx database.Tx) error
}

type Storage struct {
	engine        Engine
	wal           WAL
	dumper        Dumper
	logger        *zerolog.Logger
	cleanInterval time.Duration
	dumpInterval  time.Duration

	tx     atomic.Uint64
	dumpTx atomic.Uint64
}

func NewStorage(
	engine Engine,
	wal WAL,
	dumper Dumper,
	logger *zerolog.Logger,
	cleanInterval time.Duration,
	dumpInterval time.Duration,
) (*Storage, error) {
	if engine == nil {
		return nil, errors.New("engine is invalid")
	}

	if logger == nil {
		return nil, errors.New("logger is invalid")
	}

	return &Storage{
		engine:        engine,
		wal:           wal,
		dumper:        dumper,
		logger:        logger,
		cleanInterval: cleanInterval,
		dumpInterval:  dumpInterval,
	}, nil
}

func (s *Storage) LoadWAL(ctx context.Context, dumpLastTx database.Tx) error {
	if s.wal == nil {
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
		s.wal.Start()
	}

	go s.gcLoop(ctx)
	go s.dumpLoop(ctx)
}

func (s *Storage) Shutdown() {
	if s.wal != nil {
		s.wal.Shutdown()
	}
}

func (s *Storage) Incr(ctx context.Context, key database.BatchKey) (database.ValueType, error) {
	txCtx := s.makeTxContext()

	if s.wal != nil {
		future := s.wal.Incr(ctx, txCtx, key)
		if err := future.Get(); err != nil {
			return 0, err
		}
	}

	return s.engine.Incr(txCtx, key), nil
}

func (s *Storage) Get(_ context.Context, key database.BatchKey) (database.ValueType, error) {
	value, _ := s.engine.Get(key)

	return value, nil
}

func (s *Storage) Del(ctx context.Context, key database.BatchKey) (bool, error) {
	txCtx := s.makeTxContext()

	if s.wal != nil {
		future := s.wal.Del(ctx, txCtx, key)
		if err := future.Get(); err != nil {
			return false, err
		}
	}

	return s.engine.Del(txCtx, key), nil
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
