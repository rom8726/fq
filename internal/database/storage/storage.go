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
}

type WAL interface {
	TryRecoverWALSegments(ctx context.Context) (lastLSN uint64, err error)
	Start()
	Incr(ctx context.Context, txCtx database.TxContext, key database.BatchKey) tools.FutureError
	Del(ctx context.Context, txCtx database.TxContext, key database.BatchKey) tools.FutureError
	Shutdown()
}

type Storage struct {
	engine Engine
	wal    WAL
	logger *zerolog.Logger

	tx atomic.Uint64
}

func NewStorage(
	engine Engine,
	wal WAL,
	logger *zerolog.Logger,
) (*Storage, error) {
	if engine == nil {
		return nil, errors.New("engine is invalid")
	}

	if logger == nil {
		return nil, errors.New("logger is invalid")
	}

	return &Storage{
		engine: engine,
		wal:    wal,
		logger: logger,
	}, nil
}

func (s *Storage) LoadWAL(ctx context.Context) error {
	if s.wal == nil {
		return nil
	}

	lastLSN, err := s.wal.TryRecoverWALSegments(ctx)
	if err != nil {
		return err
	}

	s.tx.Store(lastLSN)

	return nil
}

func (s *Storage) Start(context.Context) {
	if s.wal != nil {
		s.wal.Start()
	}
}

func (s *Storage) Shutdown() {
	if s.wal != nil {
		s.wal.Shutdown()
	}
}

func (s *Storage) Incr(ctx context.Context, key database.BatchKey) (database.ValueType, error) {
	txCtx := database.TxContext{
		Tx:       database.Tx(s.tx.Add(1)),
		CurrTime: database.TxTime(time.Now().Unix()),
		FromWAL:  false,
	}

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
	txCtx := database.TxContext{
		Tx:       database.Tx(s.tx.Add(1)),
		CurrTime: database.TxTime(time.Now().Unix()),
		FromWAL:  false,
	}

	if s.wal != nil {
		future := s.wal.Del(ctx, txCtx, key)
		if err := future.Get(); err != nil {
			return false, err
		}
	}

	return s.engine.Del(txCtx, key), nil
}
