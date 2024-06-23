package dumper

import (
	"context"
	"path/filepath"

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
}

func New(engine Engine, wal WAL, dir string) *Dumper {
	return &Dumper{
		engine: engine,
		wal:    wal,
		dir:    dir,
	}
}

func (d *Dumper) currentDumpFilePath() string {
	return filepath.Join(d.dir, currentDumpFileName)
}
