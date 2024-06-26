package inmemory

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/rs/zerolog"

	"fq/internal/database"
	"fq/internal/database/compute"
	"fq/internal/database/storage/wal"
)

const (
	expireDelta = database.TxTime(60)
)

var (
	ErrInvalidArgument           = errors.New("invalid argument")
	ErrInvalidHashTablePartition = errors.New("hash table partition is invalid")
)

type hashTable interface {
	Incr(txCtx database.TxContext, key database.BatchKey) database.ValueType
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
	walStream <-chan []*wal.LogData,
	dumpStream <-chan []database.DumpElem,
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
			for logs := range walStream {
				engine.applyLogs(logs)
			}
		}()
	}

	if dumpStream != nil {
		go func() {
			for dumpElems := range dumpStream {
				engine.applyDump(dumpElems)
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
	if isExpired(elem.TxAt, database.TxTime(elem.BatchSize)) {
		return nil
	}

	idx := e.partitionIdx(elem.Key)
	partition := e.partitions[idx]
	partition.RestoreDumpElem(elem)

	return nil
}

func (e *Engine) partitionIdx(key string) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))

	return int(hash.Sum32()) % len(e.partitions)
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
		}
	}
}

func (e *Engine) applyIncrFromLog(log *wal.LogData) {
	batchKey, txCtx := parseWALBatchKeyAndCtx(log.LSN, log.Arguments[0], log.Arguments[1], log.Arguments[2])
	e.Incr(txCtx, batchKey)
}

func (e *Engine) applyDelFromLog(log *wal.LogData) {
	batchKey, txCtx := parseWALBatchKeyAndCtx(log.LSN, log.Arguments[0], log.Arguments[1], log.Arguments[2])
	e.Del(txCtx, batchKey)
}

func (e *Engine) applyMDelFromLog(log *wal.LogData) {
	var txCtx database.TxContext
	currTimeStr := log.Arguments[0]
	batchKeys := make([]database.BatchKey, 0, (len(log.Arguments)-1)/2)
	for i := 1; i < len(log.Arguments); i += 2 {
		var batchKey database.BatchKey
		batchKey, txCtx = parseWALBatchKeyAndCtx(log.LSN, log.Arguments[i], log.Arguments[i+1], currTimeStr)
		batchKeys = append(batchKeys, batchKey)
	}

	e.MDel(txCtx, batchKeys)
}

func (e *Engine) applyDump(dumpElems []database.DumpElem) {
	for _, elem := range dumpElems {
		if err := e.RestoreDumpElem(context.TODO(), elem); err != nil {
			e.logger.Error().Err(err).Msg("failed to restore dump event")
		}
	}
}

func parseWALBatchKeyAndCtx(lsn uint64, key, batchSizeStr, currTimeStr string) (database.BatchKey, database.TxContext) {
	batchSize, err := strconv.ParseUint(batchSizeStr, 10, 32)
	if err != nil {
		panic(fmt.Errorf("WAL log: parse batch size: %w", err))
	}

	currTime, err := strconv.ParseInt(currTimeStr, 16, 64)
	if err != nil {
		panic(fmt.Errorf("WAL log: parse curr time: %w", err))
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

	return batchKey, txCtx
}

func isExpired(currTime, batchSize database.TxTime) bool {
	return database.TxTime(time.Now().Unix()) > endOfBatch(currTime, batchSize)
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
