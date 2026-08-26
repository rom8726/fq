package stress

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"
)

const (
	defaultMaxMessageSize = 64 << 10
	defaultIdleTimeout    = time.Second
)

type Options struct {
	Scenario      string
	Duration      time.Duration
	Seed          int64
	KeepData      bool
	WorkDir       string
	FQBinary      string
	RepositoryDir string
}

type Environment struct {
	RootDir        string
	ConfigPath     string
	WALDir         string
	DumpDir        string
	StdoutPath     string
	StderrPath     string
	Address        string
	MaxMessageSize int
	IdleTimeout    time.Duration
	RepositoryDir  string
	FQBinary       string
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

	env := &Environment{
		RootDir:        rootDir,
		ConfigPath:     filepath.Join(rootDir, "config.yml"),
		WALDir:         filepath.Join(rootDir, "wal"),
		DumpDir:        filepath.Join(rootDir, "dump"),
		StdoutPath:     filepath.Join(rootDir, "stdout.log"),
		StderrPath:     filepath.Join(rootDir, "stderr.log"),
		Address:        address,
		MaxMessageSize: defaultMaxMessageSize,
		IdleTimeout:    defaultIdleTimeout,
		RepositoryDir:  opts.RepositoryDir,
		FQBinary:       opts.FQBinary,
	}

	if err := env.WriteConfig(); err != nil {
		return nil, err
	}

	return env, nil
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
  interval: 1h
  directory: %q
replication: {}
logging:
  level: error
`, env.Address, env.WALDir, env.DumpDir)

	if err := os.WriteFile(env.ConfigPath, []byte(data), 0o644); err != nil {
		return fmt.Errorf("write stress config: %w", err)
	}

	return nil
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
