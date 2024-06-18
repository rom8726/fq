package storage

import (
	"context"
	"errors"

	"github.com/rs/zerolog"

	"fq/internal/database"
	"fq/internal/tools"
)

type Engine interface {
	Incr(database.TxContext, database.BatchKey) database.ValueType
	Get(database.BatchKey) (database.ValueType, bool)
}

type WAL interface {
	Start()
	Incr(ctx context.Context, tx database.Tx, key database.BatchKey) tools.FutureError
	Shutdown()
}

type Storage struct {
	engine Engine
	wal    WAL
	logger *zerolog.Logger
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
	var txCtx database.TxContext // TODO: implement!

	if s.wal != nil {
		future := s.wal.Incr(ctx, txCtx.Tx, key)
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
