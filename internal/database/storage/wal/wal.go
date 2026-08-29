package wal

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/database/compute"
	"github.com/fq-db/fq/internal/observability"
	"github.com/fq-db/fq/internal/tools"
)

type fsWriter interface {
	WriteBatch([]Log)
}

type fsReader interface {
	ReadLogs(ctx context.Context) ([]*LogData, error)
	ReadSegment(ctx context.Context, filename string) ([]*LogData, error)
}

var errWALClosed = errors.New("wal is closed")

type WAL struct {
	fsWriter      fsWriter
	fsReader      fsReader
	flushTimeout  time.Duration
	maxBatchSize  int
	queueCapacity int
	directory     string

	stream chan<- Chunk

	records chan Log

	closeCh     chan struct{}
	closeDoneCh chan struct{}
	closeOnce   sync.Once
	closed      atomic.Bool

	logger *zerolog.Logger
}

func NewWAL(
	fsWriter fsWriter,
	fsReader fsReader,
	stream chan<- Chunk,
	flushTimeout time.Duration,
	maxBatchSize int,
	queueCapacity int,
	directory string,
	logger *zerolog.Logger,
) *WAL {
	if maxBatchSize <= 0 {
		maxBatchSize = 1
	}
	if queueCapacity <= 0 {
		queueCapacity = maxBatchSize
	}

	return &WAL{
		fsWriter:      fsWriter,
		fsReader:      fsReader,
		flushTimeout:  flushTimeout,
		maxBatchSize:  maxBatchSize,
		queueCapacity: queueCapacity,
		directory:     directory,
		stream:        stream,
		records:       make(chan Log, queueCapacity),
		closeCh:       make(chan struct{}),
		closeDoneCh:   make(chan struct{}),
		logger:        logger,
	}
}

func (w *WAL) Start() {
	go func() {
		defer close(w.closeDoneCh)
		defer w.closeWriter()

		batch := make([]Log, 0, w.maxBatchSize)
		timer := time.NewTimer(w.flushTimeout)
		if !timer.Stop() {
			<-timer.C
		}
		defer timer.Stop()

		var timerC <-chan time.Time

		startTimer := func() {
			if timerC != nil {
				return
			}
			timer.Reset(w.flushTimeout)
			timerC = timer.C
		}

		stopTimer := func() {
			if timerC == nil {
				return
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timerC = nil
		}

		flush := func() {
			if len(batch) == 0 {
				stopTimer()
				return
			}

			batchSize := len(batch)
			start := time.Now()
			w.fsWriter.WriteBatch(batch)
			observability.ObserveWALFlushLatency(time.Since(start))
			observability.ObserveWALFlushBatchSize(batchSize)
			observability.SetWALQueueDepth(len(w.records))
			batch = make([]Log, 0, w.maxBatchSize)
			stopTimer()
		}

		appendRecord := func(record Log) {
			if len(batch) == 0 {
				startTimer()
			}
			batch = append(batch, record)
			if len(batch) >= w.maxBatchSize {
				flush()
			}
		}

		drainReadyRecords := func() {
			for len(batch) < w.maxBatchSize {
				select {
				case record := <-w.records:
					observability.SetWALQueueDepth(len(w.records))
					if len(batch) == 0 {
						startTimer()
					}
					batch = append(batch, record)
				default:
					return
				}
			}
		}

		for {
			select {
			case <-w.closeCh:
				for {
					select {
					case record := <-w.records:
						observability.SetWALQueueDepth(len(w.records))
						appendRecord(record)
					default:
						flush()

						return
					}
				}
			case record := <-w.records:
				observability.SetWALQueueDepth(len(w.records))
				appendRecord(record)
			case <-timerC:
				timerC = nil
				drainReadyRecords()
				flush()
			}
		}
	}()
}

func (w *WAL) closeWriter() {
	writer, ok := w.fsWriter.(interface{ Close() error })
	if !ok {
		return
	}

	if err := writer.Close(); err != nil && w.logger != nil {
		w.logger.Error().Err(err).Msg("failed to close WAL writer")
	}
}

func (w *WAL) Shutdown() {
	w.closeOnce.Do(func() {
		w.closed.Store(true)
		close(w.closeCh)
	})

	// Wait for shutdown with timeout
	shutdownDone := make(chan struct{})
	go func() {
		<-w.closeDoneCh
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		// Shutdown completed
	case <-time.After(30 * time.Second):
		w.logger.Warn().Msg("WAL shutdown timeout exceeded")
	}
}

func (w *WAL) Incr(ctx context.Context, txCtx database.TxContext, key database.BatchKey) tools.FutureError {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)

	return w.push(ctx, txCtx.Tx, compute.IncrCommandID, []string{key.Key, key.BatchSizeStr, currTimeStr})
}

func (w *WAL) IncrAsync(ctx context.Context, txCtx database.TxContext, key database.BatchKey) {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)

	w.pushAsync(ctx, txCtx.Tx, compute.IncrCommandID, []string{key.Key, key.BatchSizeStr, currTimeStr})
}

func (w *WAL) Del(ctx context.Context, txCtx database.TxContext, key database.BatchKey) tools.FutureError {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)

	return w.push(ctx, txCtx.Tx, compute.DelCommandID, []string{key.Key, key.BatchSizeStr, currTimeStr})
}

func (w *WAL) DelAsync(ctx context.Context, txCtx database.TxContext, key database.BatchKey) {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)

	w.pushAsync(ctx, txCtx.Tx, compute.DelCommandID, []string{key.Key, key.BatchSizeStr, currTimeStr})
}

func (w *WAL) MDel(ctx context.Context, txCtx database.TxContext, keys []database.BatchKey) tools.FutureError {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)
	arr := make([]string, 0, len(keys)*2+1)
	arr = append(arr, currTimeStr)
	for _, key := range keys {
		arr = append(arr, key.Key, key.BatchSizeStr)
	}

	return w.push(ctx, txCtx.Tx, compute.MDelCommandID, arr)
}

func (w *WAL) MDelAsync(ctx context.Context, txCtx database.TxContext, keys []database.BatchKey) {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)
	arr := make([]string, 0, len(keys)*2+1)
	arr = append(arr, currTimeStr)
	for _, key := range keys {
		arr = append(arr, key.Key, key.BatchSizeStr)
	}

	w.pushAsync(ctx, txCtx.Tx, compute.MDelCommandID, arr)
}

func (w *WAL) RLimitSlidingWindow(
	ctx context.Context,
	txCtx database.TxContext,
	key database.BatchKey,
	limit database.ValueType,
) tools.FutureError {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)

	return w.push(ctx, txCtx.Tx, compute.RLimitSlidingWindowCommandID, []string{
		key.Key,
		strconv.FormatInt(int64(limit), 10),
		key.BatchSizeStr,
		currTimeStr,
	})
}

func (w *WAL) RLimitFixedWindow(
	ctx context.Context,
	txCtx database.TxContext,
	key database.BatchKey,
	limit database.ValueType,
) tools.FutureError {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)

	return w.push(ctx, txCtx.Tx, compute.RLimitFixedWindowCommandID, []string{
		key.Key,
		strconv.FormatInt(int64(limit), 10),
		key.BatchSizeStr,
		currTimeStr,
	})
}

func (w *WAL) RLimitFixedWindowAsync(
	ctx context.Context,
	txCtx database.TxContext,
	key database.BatchKey,
	limit database.ValueType,
) {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)

	w.pushAsync(ctx, txCtx.Tx, compute.RLimitFixedWindowCommandID, []string{
		key.Key,
		strconv.FormatInt(int64(limit), 10),
		key.BatchSizeStr,
		currTimeStr,
	})
}

func (w *WAL) RLimitSlidingWindowAsync(
	ctx context.Context,
	txCtx database.TxContext,
	key database.BatchKey,
	limit database.ValueType,
) {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)

	w.pushAsync(ctx, txCtx.Tx, compute.RLimitSlidingWindowCommandID, []string{
		key.Key,
		strconv.FormatInt(int64(limit), 10),
		key.BatchSizeStr,
		currTimeStr,
	})
}

func (w *WAL) RLimitTokenBucket(
	ctx context.Context,
	txCtx database.TxContext,
	key database.BatchKey,
	capacity database.ValueType,
	refillAmount database.ValueType,
) tools.FutureError {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)

	return w.push(ctx, txCtx.Tx, compute.RLimitTokenBucketCommandID, []string{
		key.Key,
		strconv.FormatInt(int64(capacity), 10),
		strconv.FormatInt(int64(refillAmount), 10),
		key.BatchSizeStr,
		currTimeStr,
	})
}

func (w *WAL) RLimitTokenBucketAsync(
	ctx context.Context,
	txCtx database.TxContext,
	key database.BatchKey,
	capacity database.ValueType,
	refillAmount database.ValueType,
) {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)

	w.pushAsync(ctx, txCtx.Tx, compute.RLimitTokenBucketCommandID, []string{
		key.Key,
		strconv.FormatInt(int64(capacity), 10),
		strconv.FormatInt(int64(refillAmount), 10),
		key.BatchSizeStr,
		currTimeStr,
	})
}

func (w *WAL) QuotaAcquire(
	ctx context.Context,
	txCtx database.TxContext,
	request database.QuotaAcquireRequest,
) tools.FutureError {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)
	expiresAtStr := strconv.FormatUint(uint64(request.ExpiresAt), 16)

	return w.push(ctx, txCtx.Tx, compute.QuotaAcquireCommandID, []string{
		request.Name,
		strconv.FormatInt(int64(request.Limit), 10),
		strconv.FormatInt(int64(request.Amount), 10),
		request.ClientID,
		expiresAtStr,
		currTimeStr,
	})
}

func (w *WAL) QuotaAcquireAsync(
	ctx context.Context,
	txCtx database.TxContext,
	request database.QuotaAcquireRequest,
) {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)
	expiresAtStr := strconv.FormatUint(uint64(request.ExpiresAt), 16)

	w.pushAsync(ctx, txCtx.Tx, compute.QuotaAcquireCommandID, []string{
		request.Name,
		strconv.FormatInt(int64(request.Limit), 10),
		strconv.FormatInt(int64(request.Amount), 10),
		request.ClientID,
		expiresAtStr,
		currTimeStr,
	})
}

func (w *WAL) QuotaRelease(ctx context.Context, txCtx database.TxContext, name, clientID string) tools.FutureError {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)

	return w.push(ctx, txCtx.Tx, compute.QuotaReleaseCommandID, []string{name, clientID, currTimeStr})
}

func (w *WAL) QuotaReleaseAsync(ctx context.Context, txCtx database.TxContext, name, clientID string) {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)

	w.pushAsync(ctx, txCtx.Tx, compute.QuotaReleaseCommandID, []string{name, clientID, currTimeStr})
}

func (w *WAL) QuotaDelete(ctx context.Context, txCtx database.TxContext, name string) tools.FutureError {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)

	return w.push(ctx, txCtx.Tx, compute.QuotaDeleteCommandID, []string{name, currTimeStr})
}

func (w *WAL) QuotaDeleteAsync(ctx context.Context, txCtx database.TxContext, name string) {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)

	w.pushAsync(ctx, txCtx.Tx, compute.QuotaDeleteCommandID, []string{name, currTimeStr})
}

func (w *WAL) push(
	ctx context.Context,
	tx database.Tx,
	commandID compute.CommandID,
	args []string,
) tools.FutureError {
	record := NewLog(uint64(tx), commandID, args)
	future := record.Result()

	if err := ctx.Err(); err != nil {
		record.SetResult(err)
		record.ReleaseLogData()

		return future
	}

	if w.closed.Load() {
		record.SetResult(errWALClosed)
		record.ReleaseLogData()

		return future
	}

	select {
	case <-ctx.Done():
		record.SetResult(ctx.Err())
		record.ReleaseLogData()
	case <-w.closeCh:
		record.SetResult(errWALClosed)
		record.ReleaseLogData()
	case w.records <- record:
		observability.SetWALQueueDepth(len(w.records))
	}

	return future
}

func (w *WAL) pushAsync(
	ctx context.Context,
	tx database.Tx,
	commandID compute.CommandID,
	args []string,
) {
	record := NewAsyncLog(uint64(tx), commandID, args)

	if ctx.Err() != nil {
		record.ReleaseLogData()

		return
	}

	if w.closed.Load() {
		record.ReleaseLogData()

		return
	}

	select {
	case <-ctx.Done():
		record.ReleaseLogData()
	case <-w.closeCh:
		record.ReleaseLogData()
	case w.records <- record:
		observability.SetWALQueueDepth(len(w.records))
	}
}
