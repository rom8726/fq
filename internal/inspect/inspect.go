package inspect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/fq-db/fq/internal/config"
	"github.com/fq-db/fq/internal/database/storage"
	"github.com/fq-db/fq/internal/database/storage/dumper"
	"github.com/fq-db/fq/internal/database/storage/replication"
	"github.com/fq-db/fq/internal/database/storage/wal"
)

const (
	sectionSummary = "SUMMARY"
	sectionAll     = "ALL"
	sectionWAL     = "WAL"
	sectionDump    = "DUMP"
	sectionRepl    = "REPL"
	sectionEngine  = "ENGINE"
	sectionStreams = "STREAMS"
)

var ErrUnknownSection = errors.New("unknown inspect section")

type Deps struct {
	Cfg       config.Config
	Storage   *storage.Storage
	WAL       *wal.WAL
	Dumper    *dumper.Dumper
	Master    *replication.Master
	Slave     *replication.Slave
	StartedAt time.Time
}

type Inspector struct {
	deps Deps
}

func New(deps Deps) *Inspector {
	return &Inspector{deps: deps}
}

func (i *Inspector) Report(_ context.Context, sectionArg string) ([]byte, error) {
	section := sectionSummary
	if trimmed := strings.TrimSpace(sectionArg); trimmed != "" {
		section = strings.ToUpper(trimmed)
	}

	switch section {
	case sectionSummary:
		return marshal(i.buildReport("summary", true))
	case sectionAll:
		return marshal(i.buildReport("all", false))
	case sectionWAL:
		snap := i.snapshot()
		return marshal(&Report{Section: "wal", TS: now(), WAL: i.buildWAL(snap)})
	case sectionDump:
		snap := i.snapshot()
		return marshal(&Report{Section: "dump", TS: now(), Dump: i.buildDump(snap)})
	case sectionRepl:
		snap := i.snapshot()
		return marshal(&Report{Section: "repl", TS: now(), Repl: i.buildRepl(snap, false)})
	case sectionEngine:
		return marshal(&Report{Section: "engine", TS: now(), Engine: i.buildEngine(true)})
	case sectionStreams:
		return marshal(&Report{Section: "streams", TS: now(), Streams: i.buildStreams()})
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownSection, sectionArg)
	}
}

func (i *Inspector) buildReport(sectionName string, truncate bool) *Report {
	snap := i.snapshot()

	report := &Report{
		Section:     sectionName,
		TS:          now(),
		Instance:    i.buildInstance(snap),
		Persistence: i.buildPersistence(),
		WAL:         i.buildWAL(snap),
		Dump:        i.buildDump(snap),
		Repl:        i.buildRepl(snap, truncate),
		Engine:      i.buildEngine(!truncate),
		Streams:     i.buildStreams(),
	}
	report.Warnings = buildWarnings(report)

	return report
}

func now() int64 {
	return time.Now().Unix()
}

func marshal(report *Report) ([]byte, error) {
	return json.Marshal(report)
}
