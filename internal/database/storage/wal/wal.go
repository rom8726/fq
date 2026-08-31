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

type truncatingFSWriter interface {
	Truncate() error
}

type segmentInspectableFSWriter interface {
	SegmentInfo() (path string, size int)
	LastSyncedLSN() uint64
}

type fsReader interface {
	ReadLogs(ctx context.Context) ([]*LogData, error)
	ReadSegment(ctx context.Context, filename string) ([]*LogData, error)
}

type afterLSNFSReader interface {
	ReadLogsAfter(ctx context.Context, lsn uint64) ([]*LogData, error)
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

	records  chan Log
	controls chan walControl

	closeCh     chan struct{}
	closeDoneCh chan struct{}
	closeOnce   sync.Once
	closed      atomic.Bool

	lastFlushAtUnixNano atomic.Int64
	lastFlushDurationNs atomic.Int64

	logger *zerolog.Logger
}

type walControl struct {
	fn      func() error
	promise tools.Promise[error]
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
		controls:      make(chan walControl),
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
			elapsed := time.Since(start)
			observability.ObserveWALFlushLatency(elapsed)
			observability.ObserveWALFlushBatchSize(batchSize)
			observability.SetWALQueueDepth(len(w.records))
			w.lastFlushAtUnixNano.Store(start.UnixNano())
			w.lastFlushDurationNs.Store(int64(elapsed))
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

		drainAndFlushReadyRecords := func() {
			for {
				for len(batch) < w.maxBatchSize {
					select {
					case record := <-w.records:
						observability.SetWALQueueDepth(len(w.records))
						appendRecord(record)
					default:
						flush()

						return
					}
				}
				flush()
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
			case control := <-w.controls:
				drainAndFlushReadyRecords()
				control.promise.Set(control.fn())
			case <-timerC:
				timerC = nil
				drainReadyRecords()
				flush()
			}
		}
	}()
}

func (w *WAL) QueueDepth() int {
	return len(w.records)
}

func (w *WAL) QueueCapacity() int {
	return w.queueCapacity
}

func (w *WAL) Directory() string {
	return w.directory
}

func (w *WAL) LastFlush() (at time.Time, duration time.Duration, ok bool) {
	unixNano := w.lastFlushAtUnixNano.Load()
	if unixNano == 0 {
		return time.Time{}, 0, false
	}

	return time.Unix(0, unixNano), time.Duration(w.lastFlushDurationNs.Load()), true
}

func (w *WAL) SegmentInfo() (path string, size int, ok bool) {
	writer, ok := w.fsWriter.(segmentInspectableFSWriter)
	if !ok {
		return "", 0, false
	}

	path, size = writer.SegmentInfo()

	return path, size, true
}

func (w *WAL) LastSyncedLSN() (lsn uint64, ok bool) {
	writer, ok := w.fsWriter.(segmentInspectableFSWriter)
	if !ok {
		return 0, false
	}

	return writer.LastSyncedLSN(), true
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

	args := []string{
		request.Name,
		strconv.FormatInt(int64(request.Limit), 10),
		strconv.FormatInt(int64(request.Amount), 10),
		request.ClientID,
		expiresAtStr,
		currTimeStr,
	}
	if request.Policy == database.QuotaPolicyPerClient {
		args = append(args, strconv.FormatUint(uint64(request.Policy), 10))
	}

	return w.push(ctx, txCtx.Tx, compute.QuotaAcquireCommandID, args)
}

func (w *WAL) QuotaAcquireAsync(
	ctx context.Context,
	txCtx database.TxContext,
	request database.QuotaAcquireRequest,
) {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)
	expiresAtStr := strconv.FormatUint(uint64(request.ExpiresAt), 16)

	args := []string{
		request.Name,
		strconv.FormatInt(int64(request.Limit), 10),
		strconv.FormatInt(int64(request.Amount), 10),
		request.ClientID,
		expiresAtStr,
		currTimeStr,
	}
	if request.Policy == database.QuotaPolicyPerClient {
		args = append(args, strconv.FormatUint(uint64(request.Policy), 10))
	}

	w.pushAsync(ctx, txCtx.Tx, compute.QuotaAcquireCommandID, args)
}

func (w *WAL) QuotaSet(
	ctx context.Context,
	txCtx database.TxContext,
	request database.QuotaSetRequest,
) tools.FutureError {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)

	args := []string{
		request.Name,
		strconv.FormatInt(int64(request.Limit), 10),
		currTimeStr,
	}
	if request.Policy == database.QuotaPolicyPerClient {
		args = append(args, strconv.FormatUint(uint64(request.Clients), 10))
	}

	return w.push(ctx, txCtx.Tx, compute.QuotaSetCommandID, args)
}

func (w *WAL) QuotaSetAsync(
	ctx context.Context,
	txCtx database.TxContext,
	request database.QuotaSetRequest,
) {
	currTimeStr := strconv.FormatUint(uint64(txCtx.CurrTime), 16)

	args := []string{
		request.Name,
		strconv.FormatInt(int64(request.Limit), 10),
		currTimeStr,
	}
	if request.Policy == database.QuotaPolicyPerClient {
		args = append(args, strconv.FormatUint(uint64(request.Clients), 10))
	}

	w.pushAsync(ctx, txCtx.Tx, compute.QuotaSetCommandID, args)
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

func (w *WAL) FlushDB(ctx context.Context, txCtx database.TxContext) tools.FutureError {
	future := w.push(ctx, txCtx.Tx, compute.FlushDBCommandID, nil)

	return w.after(future, func(err error) error {
		if err != nil {
			return err
		}

		return writeLastFlushDBLSN(w.directory, uint64(txCtx.Tx))
	})
}

func (w *WAL) Truncate(ctx context.Context, txCtx database.TxContext) tools.FutureError {
	return w.control(ctx, func() error {
		writer, ok := w.fsWriter.(truncatingFSWriter)
		if ok {
			if err := writer.Truncate(); err != nil {
				return err
			}
		}

		record := NewLog(uint64(txCtx.Tx), compute.TruncateCommandID, nil)
		w.fsWriter.WriteBatch([]Log{record})
		future := record.Result()

		return future.Get()
	})
}

func (w *WAL) control(ctx context.Context, fn func() error) tools.FutureError {
	promise := tools.NewPromise[error]()
	future := promise.GetFuture()

	if err := ctx.Err(); err != nil {
		promise.Set(err)

		return future
	}

	if w.closed.Load() {
		promise.Set(errWALClosed)

		return future
	}

	select {
	case <-ctx.Done():
		promise.Set(ctx.Err())
	case <-w.closeCh:
		promise.Set(errWALClosed)
	case w.controls <- walControl{fn: fn, promise: promise}:
	}

	return future
}

func (w *WAL) after(future tools.FutureError, fn func(error) error) tools.FutureError {
	promise := tools.NewPromise[error]()
	result := promise.GetFuture()

	go func() {
		promise.Set(fn(future.Get()))
	}()

	return result
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
