package storage

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/config"
	"github.com/fq-db/fq/internal/database"
	inmemory "github.com/fq-db/fq/internal/database/storage/engine/in-memory"
)

func TestLoadWALWithoutWALContinuesAfterDumpLastTx(t *testing.T) {
	engine := &txRecordingEngine{}
	logger := zerolog.Nop()
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

	require.NoError(t, strg.LoadWAL(context.Background(), database.Tx(41)))

	_, err = strg.Incr(context.Background(), database.BatchKey{Key: "key", BatchSize: 60, BatchSizeStr: "60"})
	require.NoError(t, err)
	require.Equal(t, database.Tx(42), engine.lastTx)
}

func TestRLimitFixedWindowPublishesWhenLimitIsFilled(t *testing.T) {
	logger := zerolog.Nop()
	engine, err := inmemory.NewEngine(inmemory.HashTableBuilder, 1, &logger, nil, nil)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, unsubscribe := strg.SubscribeLimitEvents(ctx, "")
	defer unsubscribe()

	key := database.BatchKey{Key: "user_42", BatchSize: 60, BatchSizeStr: "60"}
	result, err := strg.RLimitFixedWindow(ctx, key, 2)
	require.NoError(t, err)
	require.True(t, result.Allowed)
	requireNoLimitEvent(t, events)

	result, err = strg.RLimitFixedWindow(ctx, key, 2)
	require.NoError(t, err)
	require.True(t, result.Allowed)
	require.True(t, result.LimitFilled)
	require.Equal(t, database.LimitEvent{
		Key:        "user_42",
		Window:     60,
		Current:    2,
		ResetAfter: result.ResetAfter,
	}, requireLimitEvent(t, events))

	result, err = strg.RLimitFixedWindow(ctx, key, 2)
	require.NoError(t, err)
	require.False(t, result.Allowed)
	require.False(t, result.LimitFilled)
	requireNoLimitEvent(t, events)
}

func TestLimitEventSubscriptionFiltersByPrefix(t *testing.T) {
	logger := zerolog.Nop()
	engine, err := inmemory.NewEngine(inmemory.HashTableBuilder, 1, &logger, nil, nil)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, unsubscribe := strg.SubscribeLimitEvents(ctx, "tenant_a-")
	defer unsubscribe()

	otherKey := database.BatchKey{Key: "tenant_b-user_42", BatchSize: 60, BatchSizeStr: "60"}
	_, err = strg.RLimitFixedWindow(ctx, otherKey, 1)
	require.NoError(t, err)
	requireNoLimitEvent(t, events)

	matchingKey := database.BatchKey{Key: "tenant_a-user_42", BatchSize: 60, BatchSizeStr: "60"}
	result, err := strg.RLimitFixedWindow(ctx, matchingKey, 1)
	require.NoError(t, err)
	require.True(t, result.LimitFilled)
	require.Equal(t, database.LimitEvent{
		Key:        "tenant_a-user_42",
		Window:     60,
		Current:    1,
		ResetAfter: result.ResetAfter,
	}, requireLimitEvent(t, events))
}

type txRecordingEngine struct {
	lastTx database.Tx
}

func (e *txRecordingEngine) Incr(txCtx database.TxContext, _ database.BatchKey) database.ValueType {
	e.lastTx = txCtx.Tx

	return 1
}

func (e *txRecordingEngine) RLimitFixedWindow(
	txCtx database.TxContext,
	_ database.BatchKey,
	_ database.ValueType,
	beforeApply func() error,
) (database.RateLimitResult, error) {
	e.lastTx = txCtx.Tx
	if err := beforeApply(); err != nil {
		return database.RateLimitResult{}, err
	}

	return database.RateLimitResult{Allowed: true, Current: 1}, nil
}

func (e *txRecordingEngine) RLimitSlidingWindow(
	txCtx database.TxContext,
	_ database.BatchKey,
	_ database.ValueType,
	beforeApply func() error,
) (database.RateLimitResult, error) {
	e.lastTx = txCtx.Tx
	if err := beforeApply(); err != nil {
		return database.RateLimitResult{}, err
	}

	return database.RateLimitResult{Allowed: true, Current: 1}, nil
}

func (e *txRecordingEngine) RLimitTokenBucket(
	txCtx database.TxContext,
	_ database.BatchKey,
	_ database.ValueType,
	_ database.ValueType,
	beforeApply func() error,
) (database.RateLimitResult, error) {
	e.lastTx = txCtx.Tx
	if err := beforeApply(); err != nil {
		return database.RateLimitResult{}, err
	}

	return database.RateLimitResult{Allowed: true, Current: 1}, nil
}

func (e *txRecordingEngine) QuotaAcquire(
	txCtx database.TxContext,
	_ database.QuotaAcquireRequest,
	beforeApply func() error,
) (database.QuotaAcquireResult, error) {
	e.lastTx = txCtx.Tx
	if err := beforeApply(); err != nil {
		return database.QuotaAcquireResult{}, err
	}

	return database.QuotaAcquireResult{Acquired: true, Allocated: 1}, nil
}

func (e *txRecordingEngine) QuotaSet(
	txCtx database.TxContext,
	_ database.QuotaSetRequest,
	beforeApply func() error,
) (bool, error) {
	e.lastTx = txCtx.Tx
	if err := beforeApply(); err != nil {
		return false, err
	}

	return true, nil
}

func (e *txRecordingEngine) QuotaRelease(
	txCtx database.TxContext,
	_ string,
	_ string,
	beforeApply func() error,
) (database.QuotaReleaseResult, error) {
	e.lastTx = txCtx.Tx
	if err := beforeApply(); err != nil {
		return database.QuotaReleaseResult{}, err
	}

	return database.QuotaReleaseResult{Released: true}, nil
}

func (e *txRecordingEngine) QuotaDelete(txCtx database.TxContext, _ string, beforeApply func() error) (bool, error) {
	e.lastTx = txCtx.Tx
	if err := beforeApply(); err != nil {
		return false, err
	}

	return true, nil
}

func (e *txRecordingEngine) QuotaInfo(_ database.TxTime, _ string) database.QuotaInfo {
	return database.QuotaInfo{}
}

func (e *txRecordingEngine) Get(database.BatchKey) (database.ValueType, bool) {
	return 0, false
}

func (e *txRecordingEngine) Del(database.TxContext, database.BatchKey) bool {
	return false
}

func (e *txRecordingEngine) MDel(database.TxContext, []database.BatchKey) []bool {
	return nil
}

func (e *txRecordingEngine) FlushDB() {
	e.lastTx = 0
}

func (e *txRecordingEngine) Scan(string, string, uint32) (database.ScanResult, error) {
	return database.ScanResult{}, nil
}

func (e *txRecordingEngine) Clean(context.Context) {}

func (e *txRecordingEngine) Dump(context.Context, database.Tx) (<-chan database.DumpElem, <-chan error) {
	elems := make(chan database.DumpElem)
	errs := make(chan error, 1)
	close(elems)
	close(errs)

	return elems, errs
}

func (e *txRecordingEngine) RestoreDumpElem(context.Context, database.DumpElem) error {
	return nil
}

func requireNoLimitEvent(t *testing.T, events <-chan database.LimitEvent) {
	t.Helper()

	select {
	case event := <-events:
		t.Fatalf("unexpected limit event: %+v", event)
	default:
	}
}

func requireLimitEvent(t *testing.T, events <-chan database.LimitEvent) database.LimitEvent {
	t.Helper()

	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for limit event")
	}

	return database.LimitEvent{}
}
