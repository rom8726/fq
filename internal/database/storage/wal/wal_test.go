package wal

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/tools"
)

func TestWALFlushesPendingBatchOnShutdown(t *testing.T) {
	writer := &recordingFSWriter{}
	logger := zerolog.Nop()
	wal := NewWAL(writer, nil, nil, time.Hour, 10, 10, "", &logger)
	wal.Start()

	future := wal.Incr(context.Background(), testTxContext(1), testBatchKey("key"))

	wal.Shutdown()

	requireFutureError(t, future, nil)
	require.Equal(t, []int{1}, writer.BatchSizes())
}

func TestWALShutdownClosesWriterAfterFlush(t *testing.T) {
	writer := &closeRecordingFSWriter{
		closed: make(chan struct{}),
	}
	logger := zerolog.Nop()
	wal := NewWAL(writer, nil, nil, time.Hour, 10, 10, "", &logger)
	wal.Start()

	future := wal.Incr(context.Background(), testTxContext(1), testBatchKey("key"))

	wal.Shutdown()

	requireFutureError(t, future, nil)
	require.Equal(t, []int{1}, writer.BatchSizes())
	requireClosed(t, writer.closed)
}

func TestWALRejectsPushAfterShutdown(t *testing.T) {
	writer := &recordingFSWriter{}
	logger := zerolog.Nop()
	wal := NewWAL(writer, nil, nil, time.Hour, 10, 10, "", &logger)
	wal.Start()
	wal.Shutdown()

	future := wal.Incr(context.Background(), testTxContext(1), testBatchKey("key"))

	requireFutureError(t, future, errWALClosed)
	require.Empty(t, writer.BatchSizes())
}

func TestWALRejectsPushWithCanceledContext(t *testing.T) {
	writer := &recordingFSWriter{}
	logger := zerolog.Nop()
	wal := NewWAL(writer, nil, nil, time.Hour, 10, 10, "", &logger)
	wal.Start()
	defer wal.Shutdown()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	future := wal.Incr(ctx, testTxContext(1), testBatchKey("key"))

	requireFutureError(t, future, context.Canceled)
	require.Empty(t, writer.BatchSizes())
}

func TestWALPushRespectsContextWhenBackpressured(t *testing.T) {
	writer := &blockingFSWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	logger := zerolog.Nop()
	wal := NewWAL(writer, nil, nil, time.Hour, 1, 1, "", &logger)
	wal.Start()

	first := wal.Incr(context.Background(), testTxContext(1), testBatchKey("first"))
	requireClosed(t, writer.started)

	second := wal.Incr(context.Background(), testTxContext(2), testBatchKey("second"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	third := wal.Incr(ctx, testTxContext(3), testBatchKey("third"))
	requireFutureError(t, third, context.DeadlineExceeded)

	close(writer.release)
	defer wal.Shutdown()

	requireFutureError(t, first, nil)
	requireFutureError(t, second, nil)
}

func TestWALQueueCapacityCanExceedBatchSize(t *testing.T) {
	writer := &blockingFSWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	logger := zerolog.Nop()
	wal := NewWAL(writer, nil, nil, time.Hour, 1, 2, "", &logger)
	wal.Start()

	first := wal.Incr(context.Background(), testTxContext(1), testBatchKey("first"))
	requireClosed(t, writer.started)

	second := wal.Incr(context.Background(), testTxContext(2), testBatchKey("second"))
	third := wal.Incr(context.Background(), testTxContext(3), testBatchKey("third"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	fourth := wal.Incr(ctx, testTxContext(4), testBatchKey("fourth"))
	requireFutureError(t, fourth, context.DeadlineExceeded)

	close(writer.release)
	defer wal.Shutdown()

	requireFutureError(t, first, nil)
	requireFutureError(t, second, nil)
	requireFutureError(t, third, nil)
}

func TestWALFlushTimeoutStartsAfterFirstRecord(t *testing.T) {
	writer := &notifyingFSWriter{
		wrote: make(chan struct{}, 1),
	}
	logger := zerolog.Nop()
	flushTimeout := 80 * time.Millisecond
	wal := NewWAL(writer, nil, nil, flushTimeout, 10, 10, "", &logger)
	wal.Start()
	defer wal.Shutdown()

	time.Sleep(flushTimeout * 2)

	future := wal.Incr(context.Background(), testTxContext(1), testBatchKey("key"))
	select {
	case <-writer.wrote:
		t.Fatal("WAL flushed before the adaptive timeout elapsed")
	case <-time.After(flushTimeout / 4):
	}

	requireFutureError(t, future, nil)
	require.Equal(t, []int{1}, writer.BatchSizes())
}

func TestWALShutdownUnblocksBackpressuredPush(t *testing.T) {
	writer := &blockingFSWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	logger := zerolog.Nop()
	wal := NewWAL(writer, nil, nil, time.Hour, 1, 1, "", &logger)
	wal.Start()

	first := wal.Incr(context.Background(), testTxContext(1), testBatchKey("first"))
	requireClosed(t, writer.started)

	second := wal.Incr(context.Background(), testTxContext(2), testBatchKey("second"))

	result := make(chan tools.FutureError, 1)
	go func() {
		result <- wal.Incr(context.Background(), testTxContext(3), testBatchKey("third"))
	}()

	shutdownDone := make(chan struct{})
	go func() {
		wal.Shutdown()
		close(shutdownDone)
	}()

	third := requireFutureReturned(t, result)
	requireFutureError(t, third, errWALClosed)

	close(writer.release)
	requireClosed(t, shutdownDone)
	requireFutureError(t, first, nil)
	requireFutureError(t, second, nil)
}

type recordingFSWriter struct {
	mutex      sync.Mutex
	batchSizes []int
	err        error
}

func (w *recordingFSWriter) WriteBatch(batch []Log) {
	w.mutex.Lock()
	w.batchSizes = append(w.batchSizes, len(batch))
	w.mutex.Unlock()

	acknowledgeBatch(batch, w.err)
}

func (w *recordingFSWriter) BatchSizes() []int {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	result := make([]int, len(w.batchSizes))
	copy(result, w.batchSizes)

	return result
}

type closeRecordingFSWriter struct {
	recordingFSWriter

	closeOnce sync.Once
	closed    chan struct{}
}

func (w *closeRecordingFSWriter) Close() error {
	w.closeOnce.Do(func() {
		close(w.closed)
	})

	return nil
}

type notifyingFSWriter struct {
	recordingFSWriter

	wrote chan struct{}
}

func (w *notifyingFSWriter) WriteBatch(batch []Log) {
	w.recordingFSWriter.WriteBatch(batch)

	select {
	case w.wrote <- struct{}{}:
	default:
	}
}

type blockingFSWriter struct {
	started   chan struct{}
	release   chan struct{}
	startOnce sync.Once
}

func (w *blockingFSWriter) WriteBatch(batch []Log) {
	w.startOnce.Do(func() {
		close(w.started)
	})

	<-w.release

	acknowledgeBatch(batch, nil)
}

func acknowledgeBatch(batch []Log, err error) {
	for _, log := range batch {
		log.SetResult(err)
		log.ReleaseLogData()
	}
}

func testTxContext(tx database.Tx) database.TxContext {
	return database.TxContext{
		Tx:       tx,
		CurrTime: 1,
	}
}

func testBatchKey(key string) database.BatchKey {
	return database.BatchKey{
		BatchSize:    1,
		BatchSizeStr: "1",
		Key:          key,
	}
}

func requireFutureError(t *testing.T, future tools.FutureError, expected error) {
	t.Helper()

	err := requireFutureResult(t, future)
	if expected == nil {
		require.NoError(t, err)

		return
	}

	require.ErrorIs(t, err, expected)
}

func requireFutureResult(t *testing.T, future tools.FutureError) error {
	t.Helper()

	result := make(chan error, 1)
	go func() {
		result <- future.Get()
	}()

	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("future did not complete")
	}

	return nil
}

func requireFutureReturned(t *testing.T, result <-chan tools.FutureError) tools.FutureError {
	t.Helper()

	select {
	case future := <-result:
		return future
	case <-time.After(time.Second):
		t.Fatal("push did not return")
	}

	return tools.FutureError{}
}

func requireClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("channel was not closed")
	}
}
