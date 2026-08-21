// Package config is a configuration package
package config

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"gopkg.in/yaml.v3"

	"fq/internal/tools"
)

const (
	WALSyncCommitOn  = "on"
	WALSyncCommitOff = "off"

	PersistenceModeWALAndDump = "wal_and_dump"
	PersistenceModeDumpOnly   = "dump_only"
	PersistenceModeMemory     = "memory"

	ReplicaTypeMaster = "master"
	ReplicaTypeSlave  = "slave"

	configDefaultFilePath = "config.yml"
)

type Config struct {
	Engine      EngineConfig      `yaml:"engine"`
	Persistence PersistenceConfig `yaml:"persistence"`
	WAL         *WALConfig        `yaml:"wal"`
	Network     NetworkConfig     `yaml:"network"`
	Logging     LoggingConfig     `yaml:"logging"`
	Dump        DumpConfig        `yaml:"dump"`
	Replication ReplicationConfig `yaml:"replication"`
}

func (cfg Config) PersistenceMode() string {
	if cfg.Persistence.Mode == "" {
		return PersistenceModeWALAndDump
	}

	return cfg.Persistence.Mode
}

func (cfg Config) UsesWAL() bool {
	return cfg.PersistenceMode() == PersistenceModeWALAndDump
}

func (cfg Config) UsesDump() bool {
	mode := cfg.PersistenceMode()

	return mode == PersistenceModeWALAndDump || mode == PersistenceModeDumpOnly
}

type PersistenceConfig struct {
	Mode string `yaml:"mode"`
}

//nolint:tagliatelle // it's ok
type NetworkConfig struct {
	Address        string        `yaml:"address"`
	MaxConnections int           `yaml:"max_connections"`
	MaxMessageSize string        `yaml:"max_message_size"`
	IdleTimeout    time.Duration `yaml:"idle_timeout"`
}

func (cfg NetworkConfig) ParseMaxMessageSize() (int, error) {
	return tools.ParseSize(cfg.MaxMessageSize)
}

type LoggingConfig struct {
	Level string `yaml:"level"`
}

type EngineConfig struct {
	Type          string        `yaml:"type"`
	CleanInterval time.Duration `yaml:"clean_interval"`
}

type WALConfig struct {
	FlushingBatchLength  int           `yaml:"flushing_batch_length"`
	FlushingBatchTimeout time.Duration `yaml:"flushing_batch_timeout"`
	MaxSegmentSize       string        `yaml:"max_segment_size"`
	DataDirectory        string        `yaml:"data_directory"`
	SyncCommit           string        `yaml:"sync_commit"`
}

type DumpConfig struct {
	Interval  time.Duration `yaml:"interval"`
	Directory string        `yaml:"directory"`
}

type ReplicationConfig struct {
	ReplicaType   string        `yaml:"replica_type"`
	MasterAddress string        `yaml:"master_address"`
	SyncInterval  time.Duration `yaml:"sync_interval"`
}

func Init() (Config, error) {
	var configPath string

	if len(os.Args) > 1 {
		configPath = os.Args[1]
	} else {
		configPath = configDefaultFilePath
	}

	info, err := os.Stat(configPath)
	if err != nil {
		return Config{}, fmt.Errorf("stat config %q: %w", configPath, err)
	}

	if info.IsDir() {
		return Config{}, fmt.Errorf("config %q is a directory", configPath)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return Config{}, fmt.Errorf("read config file: %w", err)
	}

	cfg := Config{}
	if err := decode(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("unmarshal config: %w", err)
	}

	if err := validate(&cfg); err != nil {
		return Config{}, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

func decode(data []byte, cfg *Config) error {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)

	return decoder.Decode(cfg)
}

func validate(cfg *Config) error {
	err := validation.ValidateStruct(&cfg.Persistence,
		validation.Field(
			&cfg.Persistence.Mode,
			validation.In("", PersistenceModeWALAndDump, PersistenceModeDumpOnly, PersistenceModeMemory),
		),
	)
	if err != nil {
		return fmt.Errorf("validate persistence section: %w", err)
	}

	err = validation.ValidateStruct(&cfg.Engine,
		validation.Field(&cfg.Engine.Type, validation.Required, validation.In("in_memory")),
		validation.Field(&cfg.Engine.CleanInterval, validation.Required, positiveDurationRule),
	)
	if err != nil {
		return fmt.Errorf("validate engine section: %w", err)
	}

	if cfg.UsesDump() {
		err = validation.ValidateStruct(&cfg.Dump,
			validation.Field(&cfg.Dump.Interval, validation.Required, positiveDurationRule),
			validation.Field(&cfg.Dump.Directory, validation.Required),
		)
		if err != nil {
			return fmt.Errorf("validate dump section: %w", err)
		}
	}

	err = validation.ValidateStruct(&cfg.Network,
		validation.Field(&cfg.Network.Address, validation.Required, addressRule),
		validation.Field(&cfg.Network.MaxConnections, validation.Required, validation.Min(1)),
		validation.Field(&cfg.Network.MaxMessageSize, validation.Required, sizeRule),
		validation.Field(&cfg.Network.IdleTimeout, validation.Required, positiveDurationRule),
	)
	if err != nil {
		return fmt.Errorf("validate network section: %w", err)
	}

	if cfg.UsesWAL() && cfg.WAL == nil {
		return fmt.Errorf("wal section is required for persistence mode %q", cfg.PersistenceMode())
	}

	if cfg.UsesWAL() {
		err = validateWALConfig(cfg.WAL)
	}
	if err != nil {
		return err
	}

	err = validation.ValidateStruct(&cfg.Logging,
		validation.Field(&cfg.Logging.Level, validation.Required,
			validation.In("debug", "info", "warn", "error")),
	)
	if err != nil {
		return fmt.Errorf("validate logging section: %w", err)
	}

	err = validation.ValidateStruct(&cfg.Replication,
		validation.Field(&cfg.Replication.ReplicaType, validation.In("", ReplicaTypeMaster, ReplicaTypeSlave)),
		validation.Field(&cfg.Replication.MasterAddress, addressIfSetRule),
		validation.Field(&cfg.Replication.SyncInterval, nonNegativeDurationRule),
	)
	if err != nil {
		return fmt.Errorf("validate replication section: %w", err)
	}

	if cfg.Replication.ReplicaType != "" && cfg.PersistenceMode() != PersistenceModeWALAndDump {
		return fmt.Errorf(
			"replication requires persistence mode %q, got %q",
			PersistenceModeWALAndDump,
			cfg.PersistenceMode(),
		)
	}

	return nil
}

func validateWALConfig(cfg *WALConfig) error {
	err := validation.ValidateStruct(cfg,
		validation.Field(&cfg.FlushingBatchLength, validation.Required, validation.Min(1)),
		validation.Field(&cfg.FlushingBatchTimeout, validation.Required, positiveDurationRule),
		validation.Field(&cfg.MaxSegmentSize, validation.Required, sizeRule),
		validation.Field(&cfg.DataDirectory, validation.Required),
		validation.Field(&cfg.SyncCommit, validation.Required, validation.In(WALSyncCommitOn, WALSyncCommitOff)),
	)
	if err != nil {
		return fmt.Errorf("validate wal section: %w", err)
	}

	return nil
}

var sizeRule = validation.By(func(value interface{}) error {
	text, ok := value.(string)
	if !ok {
		return fmt.Errorf("must be a string")
	}

	size, err := tools.ParseSize(text)
	if err != nil {
		return fmt.Errorf("must be a valid size")
	}

	if size <= 0 {
		return fmt.Errorf("must be positive")
	}

	return nil
})

var addressRule = validation.By(func(value interface{}) error {
	address, ok := value.(string)
	if !ok {
		return fmt.Errorf("must be a string")
	}

	return validateAddress(address)
})

var addressIfSetRule = validation.By(func(value interface{}) error {
	address, ok := value.(string)
	if !ok {
		return fmt.Errorf("must be a string")
	}

	if address == "" {
		return nil
	}

	return validateAddress(address)
})

var positiveDurationRule = validation.By(func(value interface{}) error {
	duration, ok := value.(time.Duration)
	if !ok {
		return fmt.Errorf("must be a duration")
	}

	if duration <= 0 {
		return fmt.Errorf("must be positive")
	}

	return nil
})

var nonNegativeDurationRule = validation.By(func(value interface{}) error {
	duration, ok := value.(time.Duration)
	if !ok {
		return fmt.Errorf("must be a duration")
	}

	if duration < 0 {
		return fmt.Errorf("must be non-negative")
	}

	return nil
})

func validateAddress(address string) error {
	if address == "" {
		return fmt.Errorf("must not be empty")
	}

	_, portText, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("must be host:port")
	}

	port, err := strconv.Atoi(portText)
	if err != nil {
		return fmt.Errorf("port must be numeric")
	}

	if port <= 0 || port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}

	return nil
}
