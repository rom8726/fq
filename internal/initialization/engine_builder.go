package initialization

import (
	"errors"

	"github.com/rs/zerolog"

	"fq/internal/config"
	"fq/internal/database"
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
	defaultPartitionsNumber = 10
)

func CreateEngine(
	cfg config.EngineConfig,
	logger *zerolog.Logger,
	walStream <-chan []*wal.LogData,
	dumpStream <-chan []database.DumpElem,
) (storage.Engine, error) {
	if cfg.Type != "" {
		_, found := supportedEngineTypes[cfg.Type]
		if !found {
			return nil, errors.New("engine type is incorrect")
		}
	}

	return inMemory.NewEngine(inMemory.HashTableBuilder, defaultPartitionsNumber, logger, walStream, dumpStream)
}
