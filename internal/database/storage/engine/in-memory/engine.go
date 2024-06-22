package inmemory

import (
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

var (
	ErrInvalidArgument           = errors.New("invalid argument")
	ErrInvalidHashTablePartition = errors.New("hash table partition is invalid")
)

type hashTable interface {
	Incr(txCtx database.TxContext, key database.BatchKey) (database.ValueType, *FqElem)
	Get(key database.BatchKey) (database.ValueType, bool)
	Del(key database.BatchKey) bool
}

type Engine struct {
	partitions []hashTable
	logger     *zerolog.Logger
}

func NewEngine(
	tableBuilder func(sz int) hashTable,
	partitionsNumber int,
	initPartitionSize int,
	logger *zerolog.Logger,
	stream <-chan []*wal.LogData,
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
		if partition := tableBuilder(initPartitionSize); partition != nil {
			partitions[i] = partition
		} else {
			return nil, ErrInvalidHashTablePartition
		}
	}

	engine := &Engine{
		partitions: partitions,
		logger:     logger,
	}

	if stream != nil {
		go func() {
			for logs := range stream {
				engine.applyLogs(logs)
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
		}
	}
}

func (e *Engine) applyIncrFromLog(log *wal.LogData) {
	batchKey, txCtx := parseWALBatchKeyAndCtx(log.Arguments[0], log.Arguments[1], log.Arguments[2])
	e.Incr(txCtx, batchKey)
}

func (e *Engine) applyDelFromLog(log *wal.LogData) {
	batchKey, txCtx := parseWALBatchKeyAndCtx(log.Arguments[0], log.Arguments[1], log.Arguments[2])
	e.Del(txCtx, batchKey)
}

func parseWALBatchKeyAndCtx(key, batchSizeStr, currTimeStr string) (database.BatchKey, database.TxContext) {
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
		CurrTime: database.TxTime(currTime),
		FromWAL:  true,
	}

	return batchKey, txCtx
}

func isExpired(currTime, batchSize database.TxTime) bool {
	return database.TxTime(time.Now().Unix()) > endOfBatch(currTime, batchSize)
}

func startOfBatch(currTime, batchSize database.TxTime) database.TxTime {
	return currTime / batchSize * batchSize
}

func endOfBatch(currTime, batchSize database.TxTime) database.TxTime {
	return startOfBatch(currTime, batchSize) + batchSize - 1
}
