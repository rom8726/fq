package initialization

import (
	"errors"

	"github.com/rs/zerolog"

	"github.com/fq-db/fq/internal/config"
	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/database/storage"
	inMemory "github.com/fq-db/fq/internal/database/storage/engine/in-memory"
	"github.com/fq-db/fq/internal/database/storage/wal"
)

const (
	InMemoryEngine = "in_memory"
)

var supportedEngineTypes = map[string]struct{}{
	InMemoryEngine: {},
}

func CreateEngine(
	cfg config.EngineConfig,
	logger *zerolog.Logger,
	walStream <-chan wal.Chunk,
	dumpStream <-chan database.DumpChunk,
) (storage.Engine, error) {
	if cfg.Type != "" {
		_, found := supportedEngineTypes[cfg.Type]
		if !found {
			return nil, errors.New("engine type is incorrect")
		}
	}

	tableBuilder := inMemory.HashTableBuilder
	if cfg.KeyIndex {
		tableBuilder = inMemory.IndexedHashTableBuilder
	}

	return inMemory.NewEngineWithWALApplyWorkersAndKeyIndex(
		tableBuilder,
		cfg.PartitionsValue(),
		logger,
		walStream,
		dumpStream,
		cfg.WALApplyWorkersValue(),
		cfg.KeyIndex,
	)
}
