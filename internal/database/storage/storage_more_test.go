package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/config"
	"github.com/fq-db/fq/internal/database"
	inmemory "github.com/fq-db/fq/internal/database/storage/engine/in-memory"
	"github.com/fq-db/fq/internal/tools"
)

func newTestStorage(t *testing.T) *Storage {
	t.Helper()

	logger := zerolog.Nop()
	engine, err := inmemory.NewEngineWithKeyIndex(inmemory.IndexedHashTableBuilder, 1, &logger, nil, nil, true)
	require.NoError(t, err)

	strg, err := NewStorage(
		engine,
		nil,
		nil,
		nil,
		&logger,
		time.Hour,
		time.Hour,
		false,
		config.DefaultLimitEventQueueCapacity,
	)
	require.NoError(t, err)

	return strg
}

func TestGetDelMDelFlushDBTruncateScan(t *testing.T) {
	strg := newTestStorage(t)
	ctx := context.Background()

	key := database.BatchKey{Key: "k1", BatchSize: 60, BatchSizeStr: "60"}

	value, err := strg.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, database.ValueType(0), value)

	_, err = strg.Incr(ctx, key)
	require.NoError(t, err)

	value, err = strg.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, database.ValueType(1), value)

	scanResult, err := strg.Scan(ctx, "", "0", 10)
	require.NoError(t, err)
	require.Len(t, scanResult.Keys, 1)

	deleted, err := strg.Del(ctx, key)
	require.NoError(t, err)
	require.True(t, deleted)

	deleted, err = strg.Del(ctx, key)
	require.NoError(t, err)
	require.False(t, deleted)

	key2 := database.BatchKey{Key: "k2", BatchSize: 60, BatchSizeStr: "60"}
	_, err = strg.Incr(ctx, key)
	require.NoError(t, err)
	_, err = strg.Incr(ctx, key2)
	require.NoError(t, err)

	results, err := strg.MDel(ctx, []database.BatchKey{key, key2})
	require.NoError(t, err)
	require.Equal(t, []bool{true, true}, results)

	_, err = strg.Incr(ctx, key)
	require.NoError(t, err)
	require.NoError(t, strg.FlushDB(ctx))

	value, err = strg.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, database.ValueType(0), value)

	_, err = strg.Incr(ctx, key)
	require.NoError(t, err)
	require.NoError(t, strg.Truncate(ctx))

	value, err = strg.Get(ctx, key)
	require.NoError(t, err)
	require.Equal(t, database.ValueType(0), value)
	require.Equal(t, uint64(0), strg.DumpLSN())
}

func TestRLimitSlidingWindowAndTokenBucket(t *testing.T) {
	strg := newTestStorage(t)
	ctx := context.Background()

	key := database.BatchKey{Key: "sliding", BatchSize: 60, BatchSizeStr: "60"}
	result, err := strg.RLimitSlidingWindow(ctx, key, 1)
	require.NoError(t, err)
	require.True(t, result.Allowed)

	result, err = strg.RLimitSlidingWindow(ctx, key, 1)
	require.NoError(t, err)
	require.False(t, result.Allowed)

	bucketKey := database.BatchKey{Key: "bucket", BatchSize: 60, BatchSizeStr: "60"}
	result, err = strg.RLimitTokenBucket(ctx, bucketKey, 1, 1)
	require.NoError(t, err)
	require.True(t, result.Allowed)
}

func TestQuotaLifecycle(t *testing.T) {
	strg := newTestStorage(t)
	ctx := context.Background()

	changed, err := strg.QuotaSet(ctx, database.QuotaSetRequest{
		Name:   "quota",
		Limit:  10,
		Policy: database.QuotaPolicyFixed,
	})
	require.NoError(t, err)
	require.True(t, changed)

	events, unsubscribe := strg.SubscribeQuotaEvents(ctx, "")
	defer unsubscribe()

	acquireResult, err := strg.QuotaAcquire(ctx, database.QuotaAcquireRequest{
		Name:      "quota",
		Amount:    4,
		Ownership: database.QuotaOwnershipServer,
	})
	require.NoError(t, err)
	require.True(t, acquireResult.Acquired)
	require.Equal(t, "acq", requireQuotaEvent(t, events).Event)

	info, err := strg.QuotaInfo(ctx, "quota")
	require.NoError(t, err)
	require.Equal(t, database.ValueType(4), info.Used)

	released, err := strg.QuotaRelease(ctx, "quota", "")
	require.NoError(t, err)
	require.True(t, released)
	require.Equal(t, "rel", requireQuotaEvent(t, events).Event)

	deleted, err := strg.QuotaDelete(ctx, "quota")
	require.NoError(t, err)
	require.True(t, deleted)
	require.Equal(t, "del", requireQuotaEvent(t, events).Event)
}

func TestQuotaEventSubscriptionFiltersByPrefix(t *testing.T) {
	strg := newTestStorage(t)
	ctx := context.Background()

	events, unsubscribe := strg.SubscribeQuotaEvents(ctx, "tenant_a-")
	defer unsubscribe()

	_, err := strg.QuotaSet(ctx, database.QuotaSetRequest{Name: "tenant_b-quota", Limit: 10})
	require.NoError(t, err)
	_, err = strg.QuotaAcquire(ctx, database.QuotaAcquireRequest{
		Name:      "tenant_b-quota",
		Amount:    1,
		Ownership: database.QuotaOwnershipServer,
	})
	require.NoError(t, err)
	requireNoQuotaEvent(t, events)

	_, err = strg.QuotaSet(ctx, database.QuotaSetRequest{Name: "tenant_a-quota", Limit: 10})
	require.NoError(t, err)
	_, err = strg.QuotaAcquire(ctx, database.QuotaAcquireRequest{
		Name:      "tenant_a-quota",
		Amount:    1,
		Ownership: database.QuotaOwnershipServer,
	})
	require.NoError(t, err)
	require.Equal(t, "acq", requireQuotaEvent(t, events).Event)
}

func TestStartShutdownEngineStatsAndLastDump(t *testing.T) {
	strg := newTestStorage(t)
	ctx, cancel := context.WithCancel(context.Background())

	_, _, _, err := strg.LastDump()
	require.ErrorIs(t, err, ErrDumpNeverRun)

	strg.Start(ctx)
	cancel()
	strg.Shutdown()

	stats := strg.EngineStats()
	require.NotNil(t, stats.Partitions)

	require.False(t, strg.SyncCommit())
	require.Equal(t, uint64(0), strg.CurrentLSN())

	streamStats := strg.StreamStats()
	require.Empty(t, streamStats.LimitSubscribers)
	require.Empty(t, streamStats.QuotaSubscribers)
}

func TestWatchReturnsWhenValueChanges(t *testing.T) {
	strg := newTestStorage(t)
	key := database.BatchKey{Key: "watched", BatchSize: 60, BatchSizeStr: "60"}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan struct{})
	var value database.ValueType
	var watchErr error

	go func() {
		defer close(done)
		value, watchErr = strg.Watch(ctx, key)
	}()

	time.Sleep(150 * time.Millisecond)
	_, err := strg.Incr(context.Background(), key)
	require.NoError(t, err)

	<-done
	require.NoError(t, watchErr)
	require.Equal(t, database.ValueType(1), value)
}

func TestWatchReturnsOnContextCancel(t *testing.T) {
	strg := newTestStorage(t)
	key := database.BatchKey{Key: "watched2", BatchSize: 60, BatchSizeStr: "60"}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := strg.Watch(ctx, key)
	require.ErrorIs(t, err, context.Canceled)
}

func requireQuotaEvent(t *testing.T, events <-chan database.QuotaEvent) database.QuotaEvent {
	t.Helper()

	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for quota event")
	}

	return database.QuotaEvent{}
}

func requireNoQuotaEvent(t *testing.T, events <-chan database.QuotaEvent) {
	t.Helper()

	select {
	case event := <-events:
		t.Fatalf("unexpected quota event: %+v", event)
	default:
	}
}

type recordingWAL struct {
	mu    sync.Mutex
	calls []string
}

func (w *recordingWAL) record(name string) {
	w.mu.Lock()
	w.calls = append(w.calls, name)
	w.mu.Unlock()
}

func okFuture() tools.FutureError {
	promise := tools.NewPromise[error]()
	promise.Set(nil)

	return promise.GetFuture()
}

func (w *recordingWAL) Start()    { w.record("Start") }
func (w *recordingWAL) Shutdown() { w.record("Shutdown") }

func (w *recordingWAL) Incr(context.Context, database.TxContext, database.BatchKey) tools.FutureError {
	w.record("Incr")

	return okFuture()
}

func (w *recordingWAL) IncrAsync(context.Context, database.TxContext, database.BatchKey) {
	w.record("IncrAsync")
}

func (w *recordingWAL) Del(context.Context, database.TxContext, database.BatchKey) tools.FutureError {
	w.record("Del")

	return okFuture()
}

func (w *recordingWAL) DelAsync(context.Context, database.TxContext, database.BatchKey) {
	w.record("DelAsync")
}

func (w *recordingWAL) MDel(context.Context, database.TxContext, []database.BatchKey) tools.FutureError {
	w.record("MDel")

	return okFuture()
}

func (w *recordingWAL) MDelAsync(context.Context, database.TxContext, []database.BatchKey) {
	w.record("MDelAsync")
}

func (w *recordingWAL) RLimitSlidingWindow(
	context.Context, database.TxContext, database.BatchKey, database.ValueType,
) tools.FutureError {
	w.record("RLimitSlidingWindow")

	return okFuture()
}

func (w *recordingWAL) RLimitSlidingWindowAsync(
	context.Context, database.TxContext, database.BatchKey, database.ValueType,
) {
	w.record("RLimitSlidingWindowAsync")
}

func (w *recordingWAL) RLimitTokenBucket(
	context.Context, database.TxContext, database.BatchKey, database.ValueType, database.ValueType,
) tools.FutureError {
	w.record("RLimitTokenBucket")

	return okFuture()
}

func (w *recordingWAL) RLimitFixedWindow(
	context.Context, database.TxContext, database.BatchKey, database.ValueType,
) tools.FutureError {
	w.record("RLimitFixedWindow")

	return okFuture()
}

func (w *recordingWAL) RLimitFixedWindowAsync(
	context.Context, database.TxContext, database.BatchKey, database.ValueType,
) {
	w.record("RLimitFixedWindowAsync")
}

func (w *recordingWAL) RLimitTokenBucketAsync(
	context.Context, database.TxContext, database.BatchKey, database.ValueType, database.ValueType,
) {
	w.record("RLimitTokenBucketAsync")
}

func (w *recordingWAL) QuotaAcquire(
	context.Context, database.TxContext, database.QuotaAcquireRequest,
) tools.FutureError {
	w.record("QuotaAcquire")

	return okFuture()
}

func (w *recordingWAL) QuotaAcquireAsync(context.Context, database.TxContext, database.QuotaAcquireRequest) {
	w.record("QuotaAcquireAsync")
}

func (w *recordingWAL) QuotaSet(context.Context, database.TxContext, database.QuotaSetRequest) tools.FutureError {
	w.record("QuotaSet")

	return okFuture()
}

func (w *recordingWAL) QuotaSetAsync(context.Context, database.TxContext, database.QuotaSetRequest) {
	w.record("QuotaSetAsync")
}

func (w *recordingWAL) QuotaRelease(context.Context, database.TxContext, string, string) tools.FutureError {
	w.record("QuotaRelease")

	return okFuture()
}

func (w *recordingWAL) QuotaReleaseAsync(context.Context, database.TxContext, string, string) {
	w.record("QuotaReleaseAsync")
}

func (w *recordingWAL) QuotaDelete(context.Context, database.TxContext, string) tools.FutureError {
	w.record("QuotaDelete")

	return okFuture()
}

func (w *recordingWAL) QuotaDeleteAsync(context.Context, database.TxContext, string) {
	w.record("QuotaDeleteAsync")
}

func (w *recordingWAL) FlushDB(context.Context, database.TxContext) tools.FutureError {
	w.record("FlushDB")

	return okFuture()
}

func (w *recordingWAL) Truncate(context.Context, database.TxContext) tools.FutureError {
	w.record("Truncate")

	return okFuture()
}

func (w *recordingWAL) TryRecoverWALSegments(context.Context, uint64) (uint64, error) {
	return 0, nil
}

type fakeDumper struct {
	dumped    atomic.Bool
	truncated atomic.Bool
}

func (d *fakeDumper) Dump(context.Context, database.Tx) error {
	d.dumped.Store(true)

	return nil
}

func (d *fakeDumper) Truncate(context.Context) error {
	d.truncated.Store(true)

	return nil
}

func newTestStorageWithWAL(t *testing.T, wal WAL, syncCommit bool) *Storage {
	t.Helper()

	logger := zerolog.Nop()
	engine, err := inmemory.NewEngineWithKeyIndex(inmemory.IndexedHashTableBuilder, 1, &logger, nil, nil, true)
	require.NoError(t, err)

	strg, err := NewStorage(
		engine,
		wal,
		nil,
		nil,
		&logger,
		time.Hour,
		time.Hour,
		syncCommit,
		config.DefaultLimitEventQueueCapacity,
	)
	require.NoError(t, err)

	return strg
}

func TestWriteWALSyncCommitUsesSynchronousCalls(t *testing.T) {
	wal := &recordingWAL{}
	strg := newTestStorageWithWAL(t, wal, true)
	ctx := context.Background()
	key := database.BatchKey{Key: "k", BatchSize: 60, BatchSizeStr: "60"}

	_, err := strg.Incr(ctx, key)
	require.NoError(t, err)
	_, err = strg.Del(ctx, key)
	require.NoError(t, err)
	_, err = strg.MDel(ctx, []database.BatchKey{key})
	require.NoError(t, err)
	_, err = strg.RLimitFixedWindow(ctx, key, 10)
	require.NoError(t, err)
	_, err = strg.RLimitSlidingWindow(ctx, key, 10)
	require.NoError(t, err)
	_, err = strg.RLimitTokenBucket(ctx, key, 10, 1)
	require.NoError(t, err)
	_, err = strg.QuotaSet(ctx, database.QuotaSetRequest{Name: "q", Limit: 10})
	require.NoError(t, err)
	_, err = strg.QuotaAcquire(ctx, database.QuotaAcquireRequest{Name: "q", Amount: 1, Ownership: database.QuotaOwnershipServer})
	require.NoError(t, err)
	_, err = strg.QuotaRelease(ctx, "q", "")
	require.NoError(t, err)
	_, err = strg.QuotaDelete(ctx, "q")
	require.NoError(t, err)

	require.Equal(t, []string{
		"Incr", "Del", "MDel", "RLimitFixedWindow", "RLimitSlidingWindow",
		"RLimitTokenBucket", "QuotaSet", "QuotaAcquire", "QuotaRelease", "QuotaDelete",
	}, wal.calls)
}

func TestWriteWALAsyncCommitUsesAsynchronousCalls(t *testing.T) {
	wal := &recordingWAL{}
	strg := newTestStorageWithWAL(t, wal, false)
	ctx := context.Background()
	key := database.BatchKey{Key: "k", BatchSize: 60, BatchSizeStr: "60"}

	_, err := strg.Incr(ctx, key)
	require.NoError(t, err)
	_, err = strg.Del(ctx, key)
	require.NoError(t, err)
	_, err = strg.MDel(ctx, []database.BatchKey{key})
	require.NoError(t, err)
	_, err = strg.RLimitFixedWindow(ctx, key, 10)
	require.NoError(t, err)
	_, err = strg.RLimitSlidingWindow(ctx, key, 10)
	require.NoError(t, err)
	_, err = strg.RLimitTokenBucket(ctx, key, 10, 1)
	require.NoError(t, err)
	_, err = strg.QuotaSet(ctx, database.QuotaSetRequest{Name: "q", Limit: 10})
	require.NoError(t, err)
	_, err = strg.QuotaAcquire(ctx, database.QuotaAcquireRequest{Name: "q", Amount: 1, Ownership: database.QuotaOwnershipServer})
	require.NoError(t, err)
	_, err = strg.QuotaRelease(ctx, "q", "")
	require.NoError(t, err)
	_, err = strg.QuotaDelete(ctx, "q")
	require.NoError(t, err)

	require.Equal(t, []string{
		"IncrAsync", "DelAsync", "MDelAsync", "RLimitFixedWindowAsync", "RLimitSlidingWindowAsync",
		"RLimitTokenBucketAsync", "QuotaSetAsync", "QuotaAcquireAsync", "QuotaReleaseAsync", "QuotaDeleteAsync",
	}, wal.calls)
}

func TestFlushDBAndTruncateUseWALAndDumper(t *testing.T) {
	wal := &recordingWAL{}
	dumper := &fakeDumper{}
	logger := zerolog.Nop()
	engine, err := inmemory.NewEngineWithKeyIndex(inmemory.IndexedHashTableBuilder, 1, &logger, nil, nil, true)
	require.NoError(t, err)

	strg, err := NewStorage(engine, wal, dumper, nil, &logger, time.Hour, time.Hour, true, config.DefaultLimitEventQueueCapacity)
	require.NoError(t, err)

	require.NoError(t, strg.FlushDB(context.Background()))
	require.True(t, dumper.truncated.Load())
	require.Contains(t, wal.calls, "FlushDB")

	dumper.truncated.Store(false)
	require.NoError(t, strg.Truncate(context.Background()))
	require.True(t, dumper.truncated.Load())
	require.Contains(t, wal.calls, "Truncate")
}

func TestStartWithWALCallsWALStart(t *testing.T) {
	wal := &recordingWAL{}
	strg := newTestStorageWithWAL(t, wal, true)

	ctx, cancel := context.WithCancel(context.Background())
	strg.Start(ctx)
	cancel()
	strg.Shutdown()

	require.Contains(t, wal.calls, "Start")
	require.Contains(t, wal.calls, "Shutdown")
}

func TestDumpLoopRunsDumperPeriodically(t *testing.T) {
	logger := zerolog.Nop()
	engine, err := inmemory.NewEngineWithKeyIndex(inmemory.IndexedHashTableBuilder, 1, &logger, nil, nil, true)
	require.NoError(t, err)

	dumper := &fakeDumper{}
	strg, err := NewStorage(
		engine, nil, dumper, nil, &logger, time.Hour, 10*time.Millisecond, false, config.DefaultLimitEventQueueCapacity,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	strg.Start(ctx)

	require.Eventually(t, func() bool {
		return dumper.dumped.Load()
	}, time.Second, 10*time.Millisecond)

	_, _, _, err = strg.LastDump()
	require.NoError(t, err)
}
