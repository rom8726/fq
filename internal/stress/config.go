package stress

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"
)

const (
	defaultMaxMessageSize = 64 << 10
	//nolint:gosec // environment variable name, not a credential
	replicationTokenEnv   = "FQ_STRESS_REPLICATION_TOKEN"
	replicationTokenBytes = 24
	defaultIdleTimeout    = time.Second
)

type Options struct {
	Scenario       string        `json:"scenario"`
	Duration       time.Duration `json:"duration"`
	Seed           int64         `json:"seed"`
	Workers        int           `json:"workers"`
	Keys           int           `json:"keys"`
	KillInterval   time.Duration `json:"kill_interval"`
	DumpInterval   time.Duration `json:"dump_interval"`
	RequestTimeout time.Duration `json:"request_timeout"`
	SyncInterval   time.Duration `json:"sync_interval"`
	ReportFile     string        `json:"report_file,omitempty"`
	KeepData       bool          `json:"keep_data"`
	WorkDir        string        `json:"workdir,omitempty"`
	FQBinary       string        `json:"fq_binary,omitempty"`
	RepositoryDir  string        `json:"repo,omitempty"`
}

type Environment struct {
	RootDir        string
	ConfigPath     string
	WALDir         string
	DumpDir        string
	StdoutPath     string
	StderrPath     string
	ReportPath     string
	Address        string
	MaxMessageSize int
	IdleTimeout    time.Duration
	DumpInterval   time.Duration
	ReplicaType    string
	ReplicaID      string
	MasterAddress  string
	SyncInterval   time.Duration
	RepositoryDir  string
	FQBinary       string

	ReplicationToken string
}

func (env *Environment) ReplicationTokenEnv() string {
	return replicationTokenEnv + "=" + env.ReplicationToken
}

func newReplicationToken() (string, error) {
	raw := make([]byte, replicationTokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate replication token: %w", err)
	}

	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func NewEnvironment(opts Options) (*Environment, error) {
	rootDir := opts.WorkDir
	var err error
	if rootDir == "" {
		rootDir, err = os.MkdirTemp("", fmt.Sprintf("fq-stress-%d-*", opts.Seed))
		if err != nil {
			return nil, fmt.Errorf("create stress temp dir: %w", err)
		}
	} else if err := os.MkdirAll(rootDir, 0o750); err != nil {
		return nil, fmt.Errorf("create stress work dir: %w", err)
	}

	address, err := freeLocalAddress()
	if err != nil {
		return nil, err
	}

	replicationToken, err := newReplicationToken()
	if err != nil {
		return nil, err
	}

	env := &Environment{
		RootDir:        rootDir,
		ConfigPath:     filepath.Join(rootDir, "config.yml"),
		WALDir:         filepath.Join(rootDir, "wal"),
		DumpDir:        filepath.Join(rootDir, "dump"),
		StdoutPath:     filepath.Join(rootDir, "stdout.log"),
		StderrPath:     filepath.Join(rootDir, "stderr.log"),
		ReportPath:     opts.ReportFile,
		Address:        address,
		MaxMessageSize: defaultMaxMessageSize,
		IdleTimeout:    defaultIdleTimeout,
		DumpInterval:   opts.DumpInterval,
		SyncInterval:   opts.SyncInterval,
		RepositoryDir:  opts.RepositoryDir,
		FQBinary:       opts.FQBinary,

		ReplicationToken: replicationToken,
	}
	if env.DumpInterval <= 0 {
		env.DumpInterval = time.Hour
	}
	if env.SyncInterval <= 0 {
		env.SyncInterval = time.Second
	}
	if env.ReportPath == "" {
		env.ReportPath = filepath.Join(rootDir, "stress-result.json")
	}

	if err := env.WriteConfig(); err != nil {
		return nil, err
	}

	return env, nil
}

func NewReplicationEnvironment(opts Options) (master, slave *Environment, err error) {
	rootDir := opts.WorkDir
	if rootDir == "" {
		rootDir, err = os.MkdirTemp("", fmt.Sprintf("fq-stress-replication-%d-*", opts.Seed))
		if err != nil {
			return nil, nil, fmt.Errorf("create replication stress temp dir: %w", err)
		}
	} else if err := os.MkdirAll(rootDir, 0o750); err != nil {
		return nil, nil, fmt.Errorf("create replication stress work dir: %w", err)
	}

	replicationAddress, err := freeLocalAddress()
	if err != nil {
		return nil, nil, err
	}

	masterOpts := opts
	masterOpts.WorkDir = filepath.Join(rootDir, "master")
	master, err = NewEnvironment(masterOpts)
	if err != nil {
		return nil, nil, err
	}
	master.RootDir = rootDir
	master.ReportPath = opts.ReportFile
	if master.ReportPath == "" {
		master.ReportPath = filepath.Join(rootDir, "stress-result.json")
	}
	master.ReplicaType = "master"
	master.MasterAddress = replicationAddress
	if err := master.WriteConfig(); err != nil {
		return nil, nil, err
	}

	slaveOpts := opts
	slaveOpts.WorkDir = filepath.Join(rootDir, "slave")
	slave, err = NewEnvironment(slaveOpts)
	if err != nil {
		return nil, nil, err
	}
	slave.RootDir = rootDir
	slave.ReportPath = master.ReportPath
	slave.ReplicaType = "slave"
	slave.ReplicaID = "stress-replica-1"
	slave.MasterAddress = replicationAddress
	if err := slave.WriteConfig(); err != nil {
		return nil, nil, err
	}

	return master, slave, nil
}

func (env *Environment) WriteConfig() error {
	if err := os.MkdirAll(env.WALDir, 0o750); err != nil {
		return fmt.Errorf("create wal dir: %w", err)
	}
	if err := os.MkdirAll(env.DumpDir, 0o750); err != nil {
		return fmt.Errorf("create dump dir: %w", err)
	}

	data := fmt.Sprintf(`network:
  address: %q
  max_connections: 128
  max_message_size: 64KB
  idle_timeout: 1s
persistence:
  mode: wal_and_dump
observability:
  address: ""
wal:
  sync_commit: on
  flushing_batch_length: 1
  flushing_batch_timeout: 10ms
  queue_capacity: 16
  max_segment_size: 8MB
  data_directory: %q
engine:
  type: in_memory
  clean_interval: 1s
  limit_event_queue_capacity: 16
dump:
  interval: %s
  directory: %q
%s
logging:
  level: error
`, env.Address, env.WALDir, formatConfigDuration(env.dumpInterval()), env.DumpDir, env.replicationConfig())

	if err := os.WriteFile(env.ConfigPath, []byte(data), 0o644); err != nil {
		return fmt.Errorf("write stress config: %w", err)
	}

	return nil
}

func (env *Environment) dumpInterval() time.Duration {
	return env.DumpInterval
}

func (env *Environment) replicationConfig() string {
	if env.ReplicaType == "" {
		return "replication: {}"
	}

	if env.ReplicaType == "master" {
		return fmt.Sprintf(`replication:
  replica_type: master
  master_address: %q
  sync_interval: %s
  auth:
    token_env: %s`, env.MasterAddress, formatConfigDuration(env.SyncInterval), replicationTokenEnv)
	}

	return fmt.Sprintf(`replication:
  replica_type: slave
  replica_id: %q
  master_address: %q
  sync_interval: %s
  auth:
    token_env: %s`,
		env.ReplicaID, env.MasterAddress, formatConfigDuration(env.SyncInterval), replicationTokenEnv)
}

func formatConfigDuration(duration time.Duration) string {
	return duration.String()
}

func (env *Environment) Cleanup() error {
	return os.RemoveAll(env.RootDir)
}

func freeLocalAddress() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("allocate local address: %w", err)
	}
	defer func() { _ = listener.Close() }()

	return listener.Addr().String(), nil
}
