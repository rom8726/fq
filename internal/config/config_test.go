package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/security"
)

func TestValidateAcceptsValidConfig(t *testing.T) {
	cfg := validConfig()

	require.NoError(t, validate(&cfg))
}

func TestLoadReadsValidConfigFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yml")
	contents := `
engine:
  type: in_memory
  clean_interval: 1s
persistence:
  mode: memory
network:
  address: ":1945"
  max_connections: 10
  max_message_size: 4KB
  idle_timeout: 1m
logging:
  level: info
`
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o644))

	cfg, err := Load(path)
	require.NoError(t, err)
	require.Equal(t, "in_memory", cfg.Engine.Type)
	require.Equal(t, ":1945", cfg.Network.Address)
}

func TestLoadMissingFileReturnsError(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yml")
	require.Error(t, err)
}

func TestEngineLimitEventQueueCapacityDefaults(t *testing.T) {
	cfg := validConfig()
	cfg.Engine.LimitEventQueueCapacity = 0

	require.NoError(t, validate(&cfg))
	require.Equal(t, DefaultLimitEventQueueCapacity, cfg.Engine.LimitEventQueueCapacityValue())
}

func TestEnginePartitionsDefaults(t *testing.T) {
	cfg := validConfig()
	cfg.Engine.Partitions = 0

	require.NoError(t, validate(&cfg))
	require.Equal(t, DefaultEnginePartitions, cfg.Engine.PartitionsValue())
}

func TestEnginePartitionsUsesConfiguredValue(t *testing.T) {
	cfg := validConfig()
	cfg.Engine.Partitions = 32

	require.NoError(t, validate(&cfg))
	require.Equal(t, 32, cfg.Engine.PartitionsValue())
}

func TestEngineWALApplyWorkersDefaults(t *testing.T) {
	cfg := validConfig()
	cfg.Engine.WALApplyWorkers = 0

	require.NoError(t, validate(&cfg))
	require.Equal(t, DefaultEngineWALApplyWorkers, cfg.Engine.WALApplyWorkersValue())
}

func TestEngineWALApplyWorkersUsesConfiguredValue(t *testing.T) {
	cfg := validConfig()
	cfg.Engine.WALApplyWorkers = 4

	require.NoError(t, validate(&cfg))
	require.Equal(t, 4, cfg.Engine.WALApplyWorkersValue())
}

func TestEngineLimitEventQueueCapacityUsesConfiguredValue(t *testing.T) {
	cfg := validConfig()
	cfg.Engine.LimitEventQueueCapacity = 64

	require.NoError(t, validate(&cfg))
	require.Equal(t, 64, cfg.Engine.LimitEventQueueCapacityValue())
}

func TestPersistenceModeDefaultsToWALAndDump(t *testing.T) {
	cfg := validConfig()
	cfg.Persistence = PersistenceConfig{}

	require.Equal(t, PersistenceModeWALAndDump, cfg.PersistenceMode())
	require.True(t, cfg.UsesWAL())
	require.True(t, cfg.UsesDump())
	require.NoError(t, validate(&cfg))
}

func TestDecodeRejectsUnknownFields(t *testing.T) {
	data := []byte(`
network:
  address: ":1945"
  max_connections: 100
  max_message_size: 4KB
  idle_timeout: 1m
  typo: true
`)
	var cfg Config

	require.Error(t, decode(data, &cfg))
}

func TestRepositoryConfigFilesAreValid(t *testing.T) {
	for _, filename := range []string{"config.yml", "config-slave.yml"} {
		t.Run(filename, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("..", "..", filename))
			require.NoError(t, err)

			var cfg Config
			require.NoError(t, decode(data, &cfg))
			require.NoError(t, validate(&cfg))
		})
	}
}

func TestValidateRejectsInvalidNetworkConfig(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{
			name: "invalid address",
			mutate: func(cfg *Config) {
				cfg.Network.Address = "localhost"
			},
		},
		{
			name: "invalid port",
			mutate: func(cfg *Config) {
				cfg.Network.Address = "localhost:70000"
			},
		},
		{
			name: "non positive max connections",
			mutate: func(cfg *Config) {
				cfg.Network.MaxConnections = 0
			},
		},
		{
			name: "invalid max message size",
			mutate: func(cfg *Config) {
				cfg.Network.MaxMessageSize = "large"
			},
		},
		{
			name: "zero max message size",
			mutate: func(cfg *Config) {
				cfg.Network.MaxMessageSize = "0B"
			},
		},
		{
			name: "negative idle timeout",
			mutate: func(cfg *Config) {
				cfg.Network.IdleTimeout = -time.Second
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig()
			test.mutate(&cfg)

			require.Error(t, validate(&cfg))
		})
	}
}

func TestValidateRejectsInvalidEngineConfig(t *testing.T) {
	cfg := validConfig()
	cfg.Engine.LimitEventQueueCapacity = -1

	require.Error(t, validate(&cfg))
}

func TestValidateRejectsInvalidEnginePartitions(t *testing.T) {
	cfg := validConfig()
	cfg.Engine.Partitions = -1

	require.Error(t, validate(&cfg))
}

func TestValidateRejectsInvalidEngineWALApplyWorkers(t *testing.T) {
	cfg := validConfig()
	cfg.Engine.WALApplyWorkers = -1

	require.Error(t, validate(&cfg))
}

func TestValidateRejectsInvalidObservabilityConfig(t *testing.T) {
	cfg := validConfig()
	cfg.Observability.Address = "localhost"

	require.Error(t, validate(&cfg))
}

func TestValidateRejectsInvalidPersistenceConfig(t *testing.T) {
	cfg := validConfig()
	cfg.Persistence.Mode = "wal_only"

	require.Error(t, validate(&cfg))
}

func TestValidateAllowsDumpOnlyWithoutWAL(t *testing.T) {
	cfg := validConfig()
	cfg.Persistence.Mode = PersistenceModeDumpOnly
	cfg.WAL = nil
	cfg.Replication = ReplicationConfig{}

	require.False(t, cfg.UsesWAL())
	require.True(t, cfg.UsesDump())
	require.NoError(t, validate(&cfg))
}

func TestValidateAllowsMemoryWithoutWALAndDump(t *testing.T) {
	cfg := validConfig()
	cfg.Persistence.Mode = PersistenceModeMemory
	cfg.WAL = nil
	cfg.Dump = DumpConfig{}
	cfg.Replication = ReplicationConfig{}

	require.False(t, cfg.UsesWAL())
	require.False(t, cfg.UsesDump())
	require.NoError(t, validate(&cfg))
}

func TestValidateRejectsMissingWALForWALAndDump(t *testing.T) {
	cfg := validConfig()
	cfg.WAL = nil

	require.Error(t, validate(&cfg))
}

func TestValidateRejectsReplicationWithoutWALAndDump(t *testing.T) {
	tests := []string{
		PersistenceModeDumpOnly,
		PersistenceModeMemory,
	}

	for _, mode := range tests {
		t.Run(mode, func(t *testing.T) {
			cfg := validConfig()
			cfg.Persistence.Mode = mode

			require.Error(t, validate(&cfg))
		})
	}
}

func TestValidateRejectsInvalidWALConfig(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{
			name: "non positive flushing batch length",
			mutate: func(cfg *Config) {
				cfg.WAL.FlushingBatchLength = 0
			},
		},
		{
			name: "negative flushing batch timeout",
			mutate: func(cfg *Config) {
				cfg.WAL.FlushingBatchTimeout = -time.Millisecond
			},
		},
		{
			name: "negative queue capacity",
			mutate: func(cfg *Config) {
				cfg.WAL.QueueCapacity = -1
			},
		},
		{
			name: "queue capacity less than flushing batch length",
			mutate: func(cfg *Config) {
				cfg.WAL.FlushingBatchLength = 100
				cfg.WAL.QueueCapacity = 99
			},
		},
		{
			name: "invalid max segment size",
			mutate: func(cfg *Config) {
				cfg.WAL.MaxSegmentSize = "big"
			},
		},
		{
			name: "zero max segment size",
			mutate: func(cfg *Config) {
				cfg.WAL.MaxSegmentSize = "0"
			},
		},
		{
			name: "invalid sync commit",
			mutate: func(cfg *Config) {
				cfg.WAL.SyncCommit = "yes"
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig()
			test.mutate(&cfg)

			require.Error(t, validate(&cfg))
		})
	}
}

func TestValidateRejectsInvalidReplicationConfig(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{
			name: "invalid replica type",
			mutate: func(cfg *Config) {
				cfg.Replication.ReplicaType = "follower"
			},
		},
		{
			name: "invalid master address",
			mutate: func(cfg *Config) {
				cfg.Replication.MasterAddress = "localhost"
			},
		},
		{
			name: "negative sync interval",
			mutate: func(cfg *Config) {
				cfg.Replication.SyncInterval = -time.Second
			},
		},
		{
			name: "missing slave replica id",
			mutate: func(cfg *Config) {
				cfg.Replication.ReplicaID = ""
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig()
			test.mutate(&cfg)

			require.Error(t, validate(&cfg))
		})
	}
}

func TestValidateAllowsDefaultReplicationFields(t *testing.T) {
	cfg := validConfig()
	cfg.Replication = ReplicationConfig{}

	require.NoError(t, validate(&cfg))
}

func validConfig() Config {
	return Config{
		Engine: EngineConfig{
			Type:            "in_memory",
			CleanInterval:   time.Minute,
			Partitions:      DefaultEnginePartitions,
			WALApplyWorkers: DefaultEngineWALApplyWorkers,
		},
		Persistence: PersistenceConfig{
			Mode: PersistenceModeWALAndDump,
		},
		WAL: &WALConfig{
			FlushingBatchLength:  100,
			FlushingBatchTimeout: time.Millisecond,
			QueueCapacity:        400,
			MaxSegmentSize:       "10MB",
			DataDirectory:        "/tmp/fq/wal",
			SyncCommit:           WALSyncCommitOn,
		},
		Network: NetworkConfig{
			Address:        "localhost:1945",
			MaxConnections: 100,
			MaxMessageSize: "4KB",
			IdleTimeout:    time.Minute,
		},
		Observability: ObservabilityConfig{
			Address: "localhost:2112",
		},
		Logging: LoggingConfig{
			Level: "info",
		},
		Dump: DumpConfig{
			Interval:  time.Minute,
			Directory: "/tmp/fq/dump",
		},
		Replication: ReplicationConfig{
			ReplicaType:   ReplicaTypeSlave,
			ReplicaID:     "replica-test",
			MasterAddress: "localhost:1946",
			SyncInterval:  time.Second,
			Auth: &ReplicationAuthConfig{
				TokenEnv: "FQ_TEST_REPLICATION",
				secret:   security.Secret("replication-token-value"),
			},
		},
	}
}
