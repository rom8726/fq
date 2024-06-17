package inmemory

import (
	"errors"
	"hash/fnv"

	"github.com/rs/zerolog"

	"fq/internal/database"
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

	return engine, nil
}

func (e *Engine) Incr(txCtx database.TxContext, key database.BatchKey) database.ValueType {
	idx := e.partitionIdx(key.Key)
	partition := e.partitions[idx]
	value := partition.Incr(txCtx, key)

	if e.logger.GetLevel() == zerolog.DebugLevel {
		e.logger.Debug().
			Any("tx", txCtx.Tx).
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
