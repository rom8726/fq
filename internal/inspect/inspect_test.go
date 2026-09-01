package inspect

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/config"
	"github.com/fq-db/fq/internal/database/storage"
	inmemory "github.com/fq-db/fq/internal/database/storage/engine/in-memory"
)

func TestReportWithoutDepsCoversAllSections(t *testing.T) {
	inspector := New(Deps{StartedAt: time.Now()})
	ctx := context.Background()

	for _, section := range []string{"", "summary", "ALL", "wal", "DUMP", "repl", "engine", "streams"} {
		data, err := inspector.Report(ctx, section)
		require.NoError(t, err, section)

		var decoded map[string]any
		require.NoError(t, json.Unmarshal(data, &decoded), section)
	}
}

func TestReportRejectsUnknownSection(t *testing.T) {
	inspector := New(Deps{StartedAt: time.Now()})

	_, err := inspector.Report(context.Background(), "NOPE")
	require.ErrorIs(t, err, ErrUnknownSection)
}

func newTestStorageForInspect(t *testing.T) *storage.Storage {
	t.Helper()

	logger := zerolog.Nop()
	engine, err := inmemory.NewEngine(inmemory.HashTableBuilder, 1, &logger, nil, nil)
	require.NoError(t, err)

	strg, err := storage.NewStorage(
		engine, nil, nil, nil, &logger, time.Hour, time.Hour, false, config.DefaultLimitEventQueueCapacity,
	)
	require.NoError(t, err)

	return strg
}

func TestReportWithStorageAndWALConfig(t *testing.T) {
	strg := newTestStorageForInspect(t)

	deps := Deps{
		Cfg: config.Config{
			WAL: &config.WALConfig{SyncCommit: config.WALSyncCommitOn, DataDirectory: "/tmp/wal"},
			Dump: config.DumpConfig{
				Interval:  time.Hour,
				Directory: "/tmp/dump",
			},
		},
		Storage:   strg,
		StartedAt: time.Now(),
	}
	inspector := New(deps)

	report := inspector.buildReport("all", false)
	require.Equal(t, "all", report.Section)
	require.NotNil(t, report.Persistence)
	require.True(t, deps.Cfg.UsesWAL())
	require.NotNil(t, report.WAL)
	require.False(t, report.WAL.Enabled)
	require.NotNil(t, report.WAL.SyncCommit)
	require.Equal(t, config.WALSyncCommitOn, *report.WAL.SyncCommit)
	require.NotNil(t, report.Dump)
	require.True(t, report.Dump.Enabled)
	require.NotNil(t, report.Engine)
	require.NotNil(t, report.Streams)

	_, err := marshal(report)
	require.NoError(t, err)
}

func TestReportPersistenceModeMemorySkipsSyncCommit(t *testing.T) {
	deps := Deps{
		Cfg:       config.Config{Persistence: config.PersistenceConfig{Mode: config.PersistenceModeMemory}},
		StartedAt: time.Now(),
	}
	inspector := New(deps)

	report := inspector.buildReport("summary", true)
	require.Equal(t, config.PersistenceModeMemory, report.Persistence.Mode)
	require.Nil(t, report.Persistence.SyncCommit)
	require.False(t, report.WAL.Enabled)
	require.Nil(t, report.WAL.SyncCommit)
	require.False(t, report.Dump.Enabled)

	found := false
	for _, w := range report.Warnings {
		if w.Code == "no_durability" {
			found = true
		}
	}
	require.True(t, found)
}
