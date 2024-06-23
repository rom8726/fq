package wal

import (
	"context"
	"strconv"
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
	ReadLogs(ctx context.Context) ([]*LogData, error)
}

type WAL struct {
	fsWriter     fsWriter
	fsReader     fsReader
	flushTimeout time.Duration
	maxBatchSize int

	stream chan<- []*LogData

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
	stream chan<- []*LogData,
	flushTimeout time.Duration,
	maxBatchSize int,
	logger *zerolog.Logger,
) *WAL {
	return &WAL{
		fsWriter:     fsWriter,
		fsReader:     fsReader,
		flushTimeout: flushTimeout,
		maxBatchSize: maxBatchSize,
		stream:       stream,
		batches:      make(chan []Log, 1),
		closeCh:      make(chan struct{}),
		closeDoneCh:  make(chan struct{}),
		logger:       logger,
	}
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

func (w *WAL) Incr(ctx context.Context, txCtx database.TxContext, key database.BatchKey) tools.FutureError {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)

	return w.push(ctx, txCtx.Tx, compute.IncrCommandID, []string{key.Key, key.BatchSizeStr, currTimeStr})
}

func (w *WAL) Del(ctx context.Context, txCtx database.TxContext, key database.BatchKey) tools.FutureError {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)

	return w.push(ctx, txCtx.Tx, compute.DelCommandID, []string{key.Key, key.BatchSizeStr, currTimeStr})
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
	record := NewLog(uint64(tx), commandID, args)

	tools.WithLock(&w.mutex, func() {
		w.batch = append(w.batch, record)
		if len(w.batch) == w.maxBatchSize {
			w.batches <- w.batch
			w.batch = nil
		}
	})

	return record.Result()
}

func (w *WAL) TryRecoverWALSegments(ctx context.Context, dumpLastLSN uint64) (lastLSN uint64, err error) {
	logs, err := w.fsReader.ReadLogs(ctx)
	if err != nil {
		return 0, err
	}

	if len(logs) == 0 {
		return 0, nil
	}

	logIdx := len(logs) // end of slice
	for i := range logs {
		if logs[i].LSN > dumpLastLSN {
			logIdx = i

			break
		}
	}

	if logIdx < len(logs) {
		w.stream <- logs[logIdx:]

		return logs[len(logs)-1].LSN, nil
	}

	return dumpLastLSN, nil
}
