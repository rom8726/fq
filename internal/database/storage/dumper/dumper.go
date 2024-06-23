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

type Engine interface {
	Dump(context.Context, database.Tx) (<-chan database.DumpElem, <-chan error)
	RestoreDumpElem(ctx context.Context, elem database.DumpElem) error
}

type Dumper struct {
	engine Engine
	dir    string
}

func New(engine Engine, dir string) *Dumper {
	return &Dumper{
		engine: engine,
		dir:    dir,
	}
}

func (d *Dumper) currentDumpFilePath() string {
	return filepath.Join(d.dir, currentDumpFileName)
}
