package dumper

import (
	"context"
	"path/filepath"
	"sync"

	"fq/internal/database"
)

const (
	dumpBatchSize       = 1000
	currentDumpFileName = "current.dump"
)

type WAL interface {
	RemovePastSegments(ctx context.Context, lsn uint64) error
}

type Engine interface {
	Dump(context.Context, database.Tx) (<-chan database.DumpElem, <-chan error)
	RestoreDumpElem(ctx context.Context, elem database.DumpElem) error
}

type Dumper struct {
	engine Engine
	wal    WAL
	dir    string

	sessions   map[string]readSession
	sessMu     sync.Mutex
	readDumpMu sync.RWMutex
}

func New(engine Engine, wal WAL, dir string) *Dumper {
	return &Dumper{
		engine:   engine,
		wal:      wal,
		dir:      dir,
		sessions: make(map[string]readSession),
	}
}

func (d *Dumper) currentDumpFilePath() string {
	return filepath.Join(d.dir, currentDumpFileName)
}
