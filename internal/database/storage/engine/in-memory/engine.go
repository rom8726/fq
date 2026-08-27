package inmemory

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/rs/zerolog"

	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/database/compute"
	"github.com/fq-db/fq/internal/database/storage/wal"
)

const (
	expireDelta = database.TxTime(60)
)

var (
	ErrInvalidArgument           = errors.New("invalid argument")
	ErrInvalidHashTablePartition = errors.New("hash table partition is invalid")
	ErrInvalidWALData            = errors.New("invalid WAL log data")
)

type hashTable interface {
	Incr(txCtx database.TxContext, key database.BatchKey) database.ValueType
	RLimitFixedWindow(
		txCtx database.TxContext,
		key database.BatchKey,
		limit database.ValueType,
		beforeApply func() error,
	) (database.RateLimitResult, error)
	RLimitSlidingWindow(
		txCtx database.TxContext,
		key database.BatchKey,
		limit database.ValueType,
		beforeApply func() error,
	) (database.RateLimitResult, error)
	RLimitTokenBucket(
		txCtx database.TxContext,
		key database.BatchKey,
		capacity database.ValueType,
		refillAmount database.ValueType,
		beforeApply func() error,
	) (database.RateLimitResult, error)
	AddSlidingWindowEvent(txCtx database.TxContext, key database.BatchKey)
	AddTokenBucketEvent(
		txCtx database.TxContext,
		key database.BatchKey,
		capacity database.ValueType,
		refillAmount database.ValueType,
	)
	Get(key database.BatchKey) (database.ValueType, bool)
	Del(key database.BatchKey) bool
	Clean(ctx context.Context)
	Dump(ctx context.Context, dumpTx database.Tx, ch chan<- database.DumpElem)
	RestoreDumpElem(elem database.DumpElem)
}

type Engine struct {
	partitions []hashTable
	logger     *zerolog.Logger
}

func NewEngine(
	tableBuilder func() hashTable,
	partitionsNumber int,
	logger *zerolog.Logger,
	walStream <-chan wal.Chunk,
	dumpStream <-chan database.DumpChunk,
) (*Engine, error) {
	if tableBuilder == nil {
		return nil, ErrInvalidArgument
	}

	if partitionsNumber <= 0 {
		return nil, ErrInvalidArgument
	}

	if logger == nil {
		return nil, ErrInvalidArgument
	}

	partitions := make([]hashTable, partitionsNumber)
	for i := 0; i < partitionsNumber; i++ {
		if partition := tableBuilder(); partition != nil {
			partitions[i] = partition
		} else {
			return nil, ErrInvalidHashTablePartition
		}
	}

	engine := &Engine{
		partitions: partitions,
		logger:     logger,
	}

	if walStream != nil {
		go func() {
			for chunk := range walStream {
				engine.applyLogs(chunk.Logs)
				if chunk.Applied != nil {
					chunk.Applied <- nil
					close(chunk.Applied)
				}
			}
		}()
	}

	if dumpStream != nil {
		go func() {
			for dumpChunk := range dumpStream {
				err := engine.applyDump(dumpChunk.Elems)
				if dumpChunk.Applied != nil {
					dumpChunk.Applied <- err
					close(dumpChunk.Applied)
				}
			}
		}()
	}

	return engine, nil
}

func (e *Engine) Incr(txCtx database.TxContext, key database.BatchKey) database.ValueType {
	if txCtx.FromWAL && isExpired(txCtx.CurrTime, database.TxTime(key.BatchSize)) {
		// expired value
		return 0 // return 0 for WAL worker
	}

	idx := e.partitionIdx(key.Key)
	partition := e.partitions[idx]
	value := partition.Incr(txCtx, key)

	if e.logger.GetLevel() == zerolog.DebugLevel {
		e.logger.Debug().
			Any("tx_ctx", txCtx).
			Any("key", key).
			Any("value", value).
			Msg("success incr query")
	}

	return value
}

// RLimitFixedWindow ...
//
//nolint:dupl // ok
func (e *Engine) RLimitFixedWindow(
	txCtx database.TxContext,
	key database.BatchKey,
	limit database.ValueType,
	beforeApply func() error,
) (database.RateLimitResult, error) {
	if txCtx.FromWAL && isExpired(txCtx.CurrTime, database.TxTime(key.BatchSize)) {
		return database.RateLimitResult{}, nil
	}

	idx := e.partitionIdx(key.Key)
	partition := e.partitions[idx]
	result, err := partition.RLimitFixedWindow(txCtx, key, limit, beforeApply)

	if e.logger.GetLevel() == zerolog.DebugLevel {
		e.logger.Debug().
			Any("tx_ctx", txCtx).
			Any("key", key).
			Any("limit", limit).
			Any("result", result).
			Err(err).
			Msg("success rlimit fixed window query")
	}

	return result, err
}

// RLimitSlidingWindow ...
//
//nolint:dupl,gocritic // ok
func (e *Engine) RLimitSlidingWindow(
	txCtx database.TxContext,
	key database.BatchKey,
	limit database.ValueType,
	beforeApply func() error,
) (database.RateLimitResult, error) {
	if txCtx.FromWAL && isSlidingWindowEventExpired(txCtx.CurrTime, database.TxTime(key.BatchSize)) {
		return database.RateLimitResult{}, nil
	}

	idx := e.partitionIdx(key.Key)
	partition := e.partitions[idx]
	result, err := partition.RLimitSlidingWindow(txCtx, key, limit, beforeApply)

	if e.logger.GetLevel() == zerolog.DebugLevel {
		e.logger.Debug().
			Any("tx_ctx", txCtx).
			Any("key", key).
			Any("limit", limit).
			Any("result", result).
			Err(err).
			Msg("success rlimit sliding window query")
	}

	return result, err
}

// RLimitTokenBucket ...
//
//nolint:gocritic // ok
func (e *Engine) RLimitTokenBucket(
	txCtx database.TxContext,
	key database.BatchKey,
	capacity database.ValueType,
	refillAmount database.ValueType,
	beforeApply func() error,
) (database.RateLimitResult, error) {
	idx := e.partitionIdx(key.Key)
	partition := e.partitions[idx]
	result, err := partition.RLimitTokenBucket(txCtx, key, capacity, refillAmount, beforeApply)

	if e.logger.GetLevel() == zerolog.DebugLevel {
		e.logger.Debug().
			Any("tx_ctx", txCtx).
			Any("key", key).
			Any("capacity", capacity).
			Any("refill_amount", refillAmount).
			Any("result", result).
			Err(err).
			Msg("success rlimit token bucket query")
	}

	return result, err
}

func (e *Engine) Get(key database.BatchKey) (database.ValueType, bool) {
	idx := e.partitionIdx(key.Key)
	partition := e.partitions[idx]
	value, found := partition.Get(key)

	if e.logger.GetLevel() == zerolog.DebugLevel {
		e.logger.Debug().
			Any("key", key).
			Any("value", value).
			Msg("success get query")
	}

	return value, found
}

func (e *Engine) Del(txCtx database.TxContext, key database.BatchKey) bool {
	if txCtx.FromWAL && isExpired(txCtx.CurrTime, database.TxTime(key.BatchSize)) {
		return false
	}

	idx := e.partitionIdx(key.Key)
	partition := e.partitions[idx]
	res := partition.Del(key)

	if e.logger.GetLevel() == zerolog.DebugLevel {
		e.logger.Debug().
			Any("key", key).
			Bool("result", res).
			Msg("success del query")
	}

	return res
}

func (e *Engine) MDel(txCtx database.TxContext, keys []database.BatchKey) []bool {
	res := make([]bool, len(keys))
	for i, k := range keys {
		v := e.Del(txCtx, k)
		res[i] = v
	}

	return res
}

func (e *Engine) Clean(ctx context.Context) {
	for _, partition := range e.partitions {
		partition.Clean(ctx)
	}
}

func (e *Engine) Dump(ctx context.Context, dumpTx database.Tx) (resC <-chan database.DumpElem, errsC <-chan error) {
	ch := make(chan database.DumpElem, 1)
	errC := make(chan error, 1)

	go func() {
		defer close(ch)
		defer close(errC)

		for _, partition := range e.partitions {
			partition.Dump(ctx, dumpTx, ch)
		}
	}()

	return ch, errC
}

func (e *Engine) RestoreDumpElem(_ context.Context, elem database.DumpElem) error {
	if elem.Kind == database.DumpElemKindTokenBucket {
		idx := e.partitionIdx(elem.Key)
		partition := e.partitions[idx]
		partition.RestoreDumpElem(elem)

		return nil
	}

	if elem.Kind == database.DumpElemKindSlidingWindowBucket &&
		isSlidingWindowEventExpired(elem.TxAt, database.TxTime(elem.BatchSize)) {
		return nil
	}
	if elem.Kind != database.DumpElemKindSlidingWindowBucket &&
		isExpired(elem.TxAt, database.TxTime(elem.BatchSize)) {
		return nil
	}

	idx := e.partitionIdx(elem.Key)
	partition := e.partitions[idx]
	partition.RestoreDumpElem(elem)

	return nil
}

func (e *Engine) partitionIdx(key string) int {
	// Fast hash function for partition selection
	// Using simple multiplicative hash for better performance
	hash := uint32(0)
	for i := 0; i < len(key); i++ {
		hash = hash*31 + uint32(key[i])
	}
	return int(hash) % len(e.partitions)
}

//nolint:gocritic
func (e *Engine) applyLogs(logs []*wal.LogData) {
	for _, log := range logs {
		switch compute.CommandID(log.CommandId) {
		case compute.IncrCommandID:
			e.applyIncrFromLog(log)
		case compute.DelCommandID:
			e.applyDelFromLog(log)
		case compute.MDelCommandID:
			e.applyMDelFromLog(log)
		case compute.RLimitSlidingWindowCommandID:
			e.applySlidingWindowEventFromLog(log)
		case compute.RLimitTokenBucketCommandID:
			e.applyTokenBucketEventFromLog(log)
		}
	}
}

func (e *Engine) applyIncrFromLog(log *wal.LogData) {
	e.applySingleKeyLog(log, "INCR", e.Incr)
}

func (e *Engine) applyDelFromLog(log *wal.LogData) {
	e.applySingleKeyLog(log, "DEL", func(txCtx database.TxContext, key database.BatchKey) database.ValueType {
		if e.Del(txCtx, key) {
			return 1
		}

		return 0
	})
}

func (e *Engine) applySlidingWindowEventFromLog(log *wal.LogData) {
	if len(log.Arguments) < 3 {
		e.logger.Error().
			Uint64("lsn", log.LSN).
			Int("arguments_count", len(log.Arguments)).
			Str("command", "RLIMIT_SW").
			Msg("invalid WAL log: insufficient arguments")
		return
	}

	batchKey, txCtx, err := parseWALBatchKeyAndCtx(log.LSN, log.Arguments[0], log.Arguments[1], log.Arguments[2])
	if err != nil {
		e.logger.Error().Err(err).Uint64("lsn", log.LSN).Str("command", "RLIMIT_SW").Msg("failed to parse WAL log")
		return
	}
	if isSlidingWindowEventExpired(txCtx.CurrTime, database.TxTime(batchKey.BatchSize)) {
		return
	}

	idx := e.partitionIdx(batchKey.Key)
	partition := e.partitions[idx]
	partition.AddSlidingWindowEvent(txCtx, batchKey)
}

func (e *Engine) applyTokenBucketEventFromLog(log *wal.LogData) {
	if len(log.Arguments) < 5 {
		e.logger.Error().
			Uint64("lsn", log.LSN).
			Int("arguments_count", len(log.Arguments)).
			Str("command", "RLIMIT_TB").
			Msg("invalid WAL log: insufficient arguments")
		return
	}

	capacity, err := strconv.ParseUint(log.Arguments[1], 10, 31)
	if err != nil {
		e.logger.Error().Err(err).Uint64("lsn", log.LSN).Str("command", "RLIMIT_TB").Msg("failed to parse capacity")
		return
	}

	refillAmount, err := strconv.ParseUint(log.Arguments[2], 10, 31)
	if err != nil {
		e.logger.Error().Err(err).Uint64("lsn", log.LSN).Str("command", "RLIMIT_TB").Msg("failed to parse refill amount")
		return
	}
	if capacity == 0 || refillAmount == 0 {
		e.logger.Error().
			Uint64("lsn", log.LSN).
			Uint64("capacity", capacity).
			Uint64("refill_amount", refillAmount).
			Str("command", "RLIMIT_TB").
			Msg("invalid WAL log: capacity and refill amount must be positive")
		return
	}

	batchKey, txCtx, err := parseWALBatchKeyAndCtx(log.LSN, log.Arguments[0], log.Arguments[3], log.Arguments[4])
	if err != nil {
		e.logger.Error().Err(err).Uint64("lsn", log.LSN).Str("command", "RLIMIT_TB").Msg("failed to parse WAL log")
		return
	}

	idx := e.partitionIdx(batchKey.Key)
	partition := e.partitions[idx]
	partition.AddTokenBucketEvent(txCtx, batchKey, database.ValueType(capacity), database.ValueType(refillAmount))
}

func (e *Engine) applySingleKeyLog(
	log *wal.LogData,
	command string,
	apply func(database.TxContext, database.BatchKey) database.ValueType,
) {
	if len(log.Arguments) < 3 {
		e.logger.Error().
			Uint64("lsn", log.LSN).
			Int("arguments_count", len(log.Arguments)).
			Str("command", command).
			Msg("invalid WAL log: insufficient arguments")
		return
	}

	batchKey, txCtx, err := parseWALBatchKeyAndCtx(log.LSN, log.Arguments[0], log.Arguments[1], log.Arguments[2])
	if err != nil {
		e.logger.Error().Err(err).Uint64("lsn", log.LSN).Str("command", command).Msg("failed to parse WAL log")
		return
	}

	apply(txCtx, batchKey)
}

func (e *Engine) applyMDelFromLog(log *wal.LogData) {
	if len(log.Arguments) < 1 || (len(log.Arguments)-1)%2 != 0 {
		e.logger.Error().
			Uint64("lsn", log.LSN).
			Int("arguments_count", len(log.Arguments)).
			Msg("invalid WAL log: insufficient or invalid arguments for MDEL")
		return
	}

	var txCtx database.TxContext
	currTimeStr := log.Arguments[0]
	// Pre-allocate with exact capacity
	expectedKeys := (len(log.Arguments) - 1) / 2
	batchKeys := make([]database.BatchKey, 0, expectedKeys)
	for i := 1; i < len(log.Arguments); i += 2 {
		batchKey, parsedTxCtx, err := parseWALBatchKeyAndCtx(log.LSN, log.Arguments[i], log.Arguments[i+1], currTimeStr)
		if err != nil {
			e.logger.Error().Err(err).Uint64("lsn", log.LSN).Int("arg_index", i).Msg("failed to parse WAL log argument for MDEL")
			continue
		}
		txCtx = parsedTxCtx
		batchKeys = append(batchKeys, batchKey)
	}

	if len(batchKeys) > 0 {
		e.MDel(txCtx, batchKeys)
	}
}

func (e *Engine) applyDump(dumpElems []database.DumpElem) error {
	ctx := context.Background()
	var result error
	for _, elem := range dumpElems {
		if err := e.RestoreDumpElem(ctx, elem); err != nil {
			e.logger.Error().Err(err).Msg("failed to restore dump event")
			result = errors.Join(result, err)
		}
	}

	return result
}

func parseWALBatchKeyAndCtx(
	lsn uint64,
	key string,
	batchSizeStr string,
	currTimeStr string,
) (database.BatchKey, database.TxContext, error) {
	batchSize, err := strconv.ParseUint(batchSizeStr, 10, 32)
	if err != nil {
		return database.BatchKey{}, database.TxContext{}, fmt.Errorf("WAL log: parse batch size: %w", err)
	}

	currTime, err := strconv.ParseInt(currTimeStr, 16, 64)
	if err != nil {
		return database.BatchKey{}, database.TxContext{}, fmt.Errorf("WAL log: parse curr time: %w", err)
	}

	batchKey := database.BatchKey{
		BatchSize:    uint32(batchSize),
		BatchSizeStr: batchSizeStr,
		Key:          key,
	}

	txCtx := database.TxContext{
		Tx:       database.Tx(lsn),
		CurrTime: database.TxTime(currTime),
		FromWAL:  true,
	}

	return batchKey, txCtx, nil
}

func isExpired(currTime, batchSize database.TxTime) bool {
	return database.TxTime(time.Now().Unix()) > endOfBatch(currTime, batchSize)
}

func isSlidingWindowEventExpired(currTime, window database.TxTime) bool {
	return currTime+window <= database.TxTime(time.Now().Unix())
}

func isExpiredWithDelta(currTime, batchSize database.TxTime) bool {
	return database.TxTime(time.Now().Unix()) > (endOfBatch(currTime, batchSize) + expireDelta)
}

func startOfBatch(currTime, batchSize database.TxTime) database.TxTime {
	return currTime / batchSize * batchSize
}

func endOfBatch(currTime, batchSize database.TxTime) database.TxTime {
	return startOfBatch(currTime, batchSize) + batchSize - 1
}
