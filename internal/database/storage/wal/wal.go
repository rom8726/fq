package wal

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"fq/internal/database"
	"fq/internal/database/compute"
	"fq/internal/tools"
)

type fsWriter interface {
	WriteBatch([]Log)
}

type fsReader interface {
	ReadLogs() ([]LogData, error)
}

type WAL struct {
	fsWriter     fsWriter
	fsReader     fsReader
	flushTimeout time.Duration
	maxBatchSize int

	mutex   sync.Mutex
	batch   []Log
	batches chan []Log

	closeCh     chan struct{}
	closeDoneCh chan struct{}

	logger *zerolog.Logger
}

func NewWAL(
	fsWriter fsWriter,
	fsReader fsReader,
	stream chan<- []LogData,
	flushTimeout time.Duration,
	maxBatchSize int,
	logger *zerolog.Logger,
) *WAL {
	wal := &WAL{
		fsWriter:     fsWriter,
		fsReader:     fsReader,
		flushTimeout: flushTimeout,
		maxBatchSize: maxBatchSize,
		batches:      make(chan []Log, 1),
		closeCh:      make(chan struct{}),
		closeDoneCh:  make(chan struct{}),
		logger:       logger,
	}

	wal.tryRecoverWALSegments(stream)

	return wal
}

func (w *WAL) Start() {
	go func() {
		defer close(w.closeDoneCh)

		for {
			select {
			case <-w.closeCh:
				w.flushBatch()

				return
			case batch := <-w.batches:
				w.fsWriter.WriteBatch(batch)
			case <-time.After(w.flushTimeout):
				w.flushBatch()
			}
		}
	}()
}

func (w *WAL) Shutdown() {
	close(w.closeCh)
	<-w.closeDoneCh
}

func (w *WAL) Incr(ctx context.Context, tx database.Tx, key database.BatchKey) tools.FutureError {
	return w.push(ctx, tx, compute.IncrCommandID, []string{key.Key, key.BatchSizeStr})
}

func (w *WAL) flushBatch() {
	var batch []Log
	tools.WithLock(&w.mutex, func() {
		if len(w.batch) != 0 {
			batch = w.batch
			w.batch = nil
		}
	})

	if len(batch) != 0 {
		w.fsWriter.WriteBatch(batch)
	}
}

func (w *WAL) push(
	_ context.Context,
	tx database.Tx,
	commandID compute.CommandID,
	args []string,
) tools.FutureError {
	record := NewLog(int64(tx), commandID, args)

	tools.WithLock(&w.mutex, func() {
		w.batch = append(w.batch, record)
		if len(w.batch) == w.maxBatchSize {
			w.batches <- w.batch
			w.batch = nil
		}
	})

	return record.Result()
}

func (w *WAL) tryRecoverWALSegments(stream chan<- []LogData) {
	logs, err := w.fsReader.ReadLogs()
	if err != nil {
		w.logger.Error().Err(err).Msg("failed to recover WAL segments")
	} else {
		stream <- logs
	}
}
