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
	Incr(txCtx database.TxContext, key database.BatchKey) database.ValueType
	Get(key database.BatchKey) (database.ValueType, bool)
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
	stream <-chan []wal.LogData,
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
	if txCtx.FromWAL && isExpired(txCtx.CurrTime, key.BatchSize) {
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

func (e *Engine) partitionIdx(key string) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))

	return int(hash.Sum32()) % len(e.partitions)
}

//nolint:gocritic
func (e *Engine) applyLogs(logs []wal.LogData) {
	for _, log := range logs {
		if log.CommandID == compute.IncrCommandID {
			batchSize, err := strconv.ParseUint(log.Arguments[1], 10, 32)
			if err != nil {
				panic(fmt.Errorf("WAL log: parse batch size: %w", err))
			}

			currTime, err := strconv.ParseInt(log.Arguments[2], 16, 64)
			if err != nil {
				panic(fmt.Errorf("WAL log: parse curr time: %w", err))
			}

			batchKey := database.BatchKey{
				BatchSize:    uint32(batchSize),
				BatchSizeStr: log.Arguments[1],
				Key:          log.Arguments[0],
			}

			txCtx := database.TxContext{
				CurrTime: database.TxTime(currTime),
				FromWAL:  true,
			}

			e.Incr(txCtx, batchKey)
		}
	}
}

func isExpired(currTime database.TxTime, batchSize uint32) bool {
	batchEndsAt := uint32(currTime)/batchSize*batchSize + batchSize - 1

	return uint32(time.Now().Unix()) > batchEndsAt
}
