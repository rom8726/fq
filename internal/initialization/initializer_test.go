package initialization

import (
	"context"
	"crypto/x509"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/config"
	inmemory "github.com/fq-db/fq/internal/database/storage/engine/in-memory"
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

func TestCreateEngineUsesConfiguredPartitions(t *testing.T) {
	cfg := testInitializerConfig()
	cfg.Engine.Partitions = 32

	initializer, err := NewInitializer(cfg)
	require.NoError(t, err)

	engine, ok := initializer.engine.(*inmemory.Engine)
	require.True(t, ok)
	partitions := reflect.ValueOf(engine).Elem().FieldByName("partitions")
	require.Equal(t, 32, partitions.Len())
}

func TestCreateEngineUsesConfiguredWALApplyWorkers(t *testing.T) {
	cfg := testInitializerConfig()
	cfg.Engine.WALApplyWorkers = 4

	initializer, err := NewInitializer(cfg)
	require.NoError(t, err)

	engine, ok := initializer.engine.(*inmemory.Engine)
	require.True(t, ok)
	workers := reflect.ValueOf(engine).Elem().FieldByName("walApplyWorkers")
	require.Equal(t, int64(4), workers.Int())
}

func TestCreateEngineUsesConfiguredKeyIndex(t *testing.T) {
	cfg := testInitializerConfig()
	cfg.Engine.KeyIndex = true

	initializer, err := NewInitializer(cfg)
	require.NoError(t, err)

	engine, ok := initializer.engine.(*inmemory.Engine)
	require.True(t, ok)
	enabled := reflect.ValueOf(engine).Elem().FieldByName("scanIndexEnabled")
	require.True(t, enabled.Bool())
}

func TestInteractiveTLSOptionsAddEphemeralClientCertificateForMTLS(t *testing.T) {
	serverTLS, tuiTLS, err := interactiveTLSOptions(config.TLSConfig{
		CertFile:     "/certs/server.crt",
		KeyFile:      "/certs/server.key",
		ClientCAFile: "/certs/clients.crt",
		CAFile:       "/certs/ca.crt",
		ServerName:   "localhost",
	})
	require.NoError(t, err)
	require.Len(t, serverTLS.ClientCACerts, 1)
	require.Len(t, tuiTLS.Certificates, 1)
	require.Equal(t, "/certs/ca.crt", tuiTLS.CAFile)
	require.Equal(t, "localhost", tuiTLS.ServerName)

	clientCert := tuiTLS.Certificates[0].Leaf
	pool := x509.NewCertPool()
	pool.AddCert(serverTLS.ClientCACerts[0])
	_, err = clientCert.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	require.NoError(t, err)
}

func testInitializerConfig() config.Config {
	return config.Config{
		Engine: config.EngineConfig{
			Type:            "in_memory",
			CleanInterval:   time.Minute,
			Partitions:      config.DefaultEnginePartitions,
			WALApplyWorkers: config.DefaultEngineWALApplyWorkers,
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
