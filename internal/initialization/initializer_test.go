package initialization

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/config"
)

func TestNewInitializerHonorsDumpOnlyPersistence(t *testing.T) {
	cfg := testInitializerConfig()
	cfg.Persistence.Mode = config.PersistenceModeDumpOnly
	cfg.WAL = nil
	cfg.Dump.Interval = time.Millisecond
	cfg.Replication = config.ReplicationConfig{}

	initializer, err := NewInitializer(cfg)
	require.NoError(t, err)

	require.Nil(t, initializer.wal)
	require.NotNil(t, initializer.dumper)
	require.Nil(t, initializer.master)
	require.Nil(t, initializer.slave)

	strg, err := initializer.createStorageLayer()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	strg.Start(ctx)
	time.Sleep(10 * time.Millisecond)
	cancel()
	strg.Shutdown()
}

func TestNewInitializerHonorsMemoryPersistence(t *testing.T) {
	cfg := testInitializerConfig()
	cfg.Persistence.Mode = config.PersistenceModeMemory
	cfg.WAL = nil
	cfg.Dump = config.DumpConfig{}
	cfg.Replication = config.ReplicationConfig{}

	initializer, err := NewInitializer(cfg)
	require.NoError(t, err)

	require.Nil(t, initializer.wal)
	require.Nil(t, initializer.dumper)
	require.Nil(t, initializer.master)
	require.Nil(t, initializer.slave)

	strg, err := initializer.createStorageLayer()
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	strg.Start(ctx)
	cancel()
	strg.Shutdown()
}

func testInitializerConfig() config.Config {
	return config.Config{
		Engine: config.EngineConfig{
			Type:          "in_memory",
			CleanInterval: time.Minute,
		},
		Persistence: config.PersistenceConfig{
			Mode: config.PersistenceModeWALAndDump,
		},
		WAL: &config.WALConfig{
			FlushingBatchLength:  100,
			FlushingBatchTimeout: time.Millisecond,
			QueueCapacity:        400,
			MaxSegmentSize:       "10MB",
			DataDirectory:        "/tmp/fq/wal",
			SyncCommit:           config.WALSyncCommitOn,
		},
		Network: config.NetworkConfig{
			Address:        "localhost:1945",
			MaxConnections: 100,
			MaxMessageSize: "4KB",
			IdleTimeout:    time.Minute,
		},
		Logging: config.LoggingConfig{
			Level: "info",
		},
		Dump: config.DumpConfig{
			Interval:  time.Minute,
			Directory: "/tmp/fq/dump",
		},
	}
}
