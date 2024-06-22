package initialization

import (
	"errors"
	"time"

	"github.com/rs/zerolog"

	"fq/internal/config"
	"fq/internal/database/storage"
	inMemory "fq/internal/database/storage/engine/in-memory"
	"fq/internal/database/storage/wal"
)

const (
	InMemoryEngine = "in_memory"
)

var supportedEngineTypes = map[string]struct{}{
	InMemoryEngine: {},
}

const (
	defaultPartitionsNumber        = 10
	defaultPartitionSize           = 1000
	defaultPartitionCleanPause     = time.Millisecond * 5
	defaultPartitionCleanThreshold = 10000
)

func CreateEngine(
	cfg *config.EngineConfig,
	logger *zerolog.Logger,
	stream <-chan []*wal.LogData,
) (storage.Engine, error) {
	if cfg.Type != "" {
		_, found := supportedEngineTypes[cfg.Type]
		if !found {
			return nil, errors.New("engine type is incorrect")
		}
	}

	return inMemory.NewEngine(
		inMemory.HashTableBuilder,
		defaultPartitionsNumber,
		defaultPartitionSize,
		defaultPartitionCleanPause,
		defaultPartitionCleanThreshold,
		logger,
		stream,
	)
}
