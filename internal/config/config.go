// Package config is a configuration package
package config

import (
	"fmt"
	"os"
	"time"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"gopkg.in/yaml.v3"

	"fq/internal/tools"
)

const (
	WALSyncCommitOn  = "on"
	WALSyncCommitOff = "off"

	configDefaultFilePath = "config.yml"
)

type Config struct {
	Engine  EngineConfig  `yaml:"engine"`
	WAL     *WALConfig    `yaml:"wal"`
	Network NetworkConfig `yaml:"network"`
	Logging LoggingConfig `yaml:"logging"`
	Dump    DumpConfig    `yaml:"dump"`
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
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("unmarshal config: %w", err)
	}

	if err := validate(&cfg); err != nil {
		return Config{}, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

func validate(cfg *Config) error {
	err := validation.ValidateStruct(&cfg.Engine,
		validation.Field(&cfg.Engine.Type, validation.Required, validation.In("in_memory")),
		validation.Field(&cfg.Engine.CleanInterval, validation.Required),
	)
	if err != nil {
		return fmt.Errorf("validate engine section: %w", err)
	}

	err = validation.ValidateStruct(&cfg.Dump,
		validation.Field(&cfg.Dump.Interval, validation.Required),
		validation.Field(&cfg.Dump.Directory, validation.Required),
	)
	if err != nil {
		return fmt.Errorf("validate dump section: %w", err)
	}

	err = validation.ValidateStruct(&cfg.Network,
		validation.Field(&cfg.Network.Address, validation.Required),
		validation.Field(&cfg.Network.MaxConnections, validation.Required),
		validation.Field(&cfg.Network.MaxMessageSize, validation.Required),
		validation.Field(&cfg.Network.IdleTimeout, validation.Required),
	)
	if err != nil {
		return fmt.Errorf("validate network section: %w", err)
	}

	if cfg.WAL != nil {
		err = validation.ValidateStruct(cfg.WAL,
			validation.Field(&cfg.WAL.FlushingBatchLength, validation.Required),
			validation.Field(&cfg.WAL.FlushingBatchTimeout, validation.Required),
			validation.Field(&cfg.WAL.MaxSegmentSize, validation.Required),
			validation.Field(&cfg.WAL.DataDirectory, validation.Required),
			validation.Field(&cfg.WAL.SyncCommit, validation.Required, validation.In(WALSyncCommitOn, WALSyncCommitOff)),
		)
		if err != nil {
			return fmt.Errorf("validate wal section: %w", err)
		}
	}

	err = validation.ValidateStruct(&cfg.Logging,
		validation.Field(&cfg.Logging.Level, validation.Required,
			validation.In("debug", "info", "warn", "error")),
	)
	if err != nil {
		return fmt.Errorf("validate logging section: %w", err)
	}

	return nil
}
