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

	"github.com/fq-db/fq/internal/security"
	"github.com/fq-db/fq/internal/tools"
)

const (
	WALSyncCommitOn  = "on"
	WALSyncCommitOff = "off"

	PersistenceModeWALAndDump = "wal_and_dump"
	PersistenceModeDumpOnly   = "dump_only"
	PersistenceModeMemory     = "memory"

	ReplicaTypeMaster = "master"
	ReplicaTypeSlave  = "slave"

	DefaultLimitEventQueueCapacity = 16
	DefaultEnginePartitions        = 16
	DefaultEngineWALApplyWorkers   = 1

	DefaultCompressionMinFrameSize = 512
)

type Config struct {
	Engine        EngineConfig        `yaml:"engine"`
	Persistence   PersistenceConfig   `yaml:"persistence"`
	WAL           *WALConfig          `yaml:"wal"`
	Network       NetworkConfig       `yaml:"network"`
	Observability ObservabilityConfig `yaml:"observability"`
	Logging       LoggingConfig       `yaml:"logging"`
	Dump          DumpConfig          `yaml:"dump"`
	Replication   ReplicationConfig   `yaml:"replication"`
	Compression   *CompressionConfig  `yaml:"compression"`
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
	Auth           AuthConfig    `yaml:"auth"`
	TLS            TLSConfig     `yaml:"tls"`
}

type AuthConfig struct {
	Tokens []TokenConfig `yaml:"tokens"`
}

//nolint:tagliatelle // it's ok
type TokenConfig struct {
	Role      string `yaml:"role"`
	TokenEnv  string `yaml:"token_env"`
	TokenFile string `yaml:"token_file"`

	role   security.Role
	secret security.Secret
}

func (cfg TokenConfig) ResolvedRole() security.Role {
	return cfg.role
}

func (cfg TokenConfig) ResolvedSecret() security.Secret {
	return cfg.secret
}

//nolint:tagliatelle // it's ok
type ReplicationAuthConfig struct {
	TokenEnv  string `yaml:"token_env"`
	TokenFile string `yaml:"token_file"`

	secret security.Secret
}

func (cfg *ReplicationAuthConfig) ResolvedSecret() security.Secret {
	if cfg == nil {
		return ""
	}

	return cfg.secret
}

//nolint:tagliatelle // it's ok
type TLSConfig struct {
	CertFile     string `yaml:"cert_file"`
	KeyFile      string `yaml:"key_file"`
	ClientCAFile string `yaml:"client_ca_file"`
	CAFile       string `yaml:"ca_file"`
	ServerName   string `yaml:"server_name"`
	SkipVerify   bool   `yaml:"skip_verify"`
	MinVersion   string `yaml:"min_version"`
}

func (cfg TLSConfig) Options() security.TLSOptions {
	return security.TLSOptions{
		CertFile:     cfg.CertFile,
		KeyFile:      cfg.KeyFile,
		ClientCAFile: cfg.ClientCAFile,
		CAFile:       cfg.CAFile,
		ServerName:   cfg.ServerName,
		SkipVerify:   cfg.SkipVerify,
		MinVersion:   cfg.MinVersion,
	}
}

func (cfg TLSConfig) ClientOptions() security.TLSOptions {
	options := security.TLSOptions{
		CAFile:     cfg.CAFile,
		ServerName: cfg.ServerName,
		SkipVerify: cfg.SkipVerify,
		MinVersion: cfg.MinVersion,
	}

	if options.CAFile == "" && !options.SkipVerify {
		options.CAFile = cfg.CertFile
	}

	return options
}

func (cfg TLSConfig) Enabled() bool {
	return !cfg.Options().Empty()
}

func (cfg NetworkConfig) ParseMaxMessageSize() (int, error) {
	return tools.ParseSize(cfg.MaxMessageSize)
}

type LoggingConfig struct {
	Level string `yaml:"level"`
}

type ObservabilityConfig struct {
	Address string `yaml:"address"`
	Pprof   bool   `yaml:"pprof"`
}

type EngineConfig struct {
	Type                    string        `yaml:"type"`
	CleanInterval           time.Duration `yaml:"clean_interval"`
	Partitions              int           `yaml:"partitions"`
	WALApplyWorkers         int           `yaml:"wal_apply_workers"`
	LimitEventQueueCapacity int           `yaml:"limit_event_queue_capacity"`
	KeyIndex                bool          `yaml:"key_index"`
}

func (cfg EngineConfig) PartitionsValue() int {
	if cfg.Partitions == 0 {
		return DefaultEnginePartitions
	}

	return cfg.Partitions
}

func (cfg EngineConfig) LimitEventQueueCapacityValue() int {
	if cfg.LimitEventQueueCapacity == 0 {
		return DefaultLimitEventQueueCapacity
	}

	return cfg.LimitEventQueueCapacity
}

func (cfg EngineConfig) WALApplyWorkersValue() int {
	if cfg.WALApplyWorkers == 0 {
		return DefaultEngineWALApplyWorkers
	}

	return cfg.WALApplyWorkers
}

type WALConfig struct {
	FlushingBatchLength  int           `yaml:"flushing_batch_length"`
	FlushingBatchTimeout time.Duration `yaml:"flushing_batch_timeout"`
	QueueCapacity        int           `yaml:"queue_capacity"`
	MaxSegmentSize       string        `yaml:"max_segment_size"`
	DataDirectory        string        `yaml:"data_directory"`
	SyncCommit           string        `yaml:"sync_commit"`
}

type DumpConfig struct {
	Interval  time.Duration `yaml:"interval"`
	Directory string        `yaml:"directory"`
}

type CompressionConfig struct {
	WAL          string `yaml:"wal"`
	Dump         string `yaml:"dump"`
	Replication  string `yaml:"replication"`
	MinFrameSize int    `yaml:"min_frame_size"`
}

func (cfg Config) CompressionValue() CompressionConfig {
	if cfg.Compression == nil {
		return CompressionConfig{}
	}

	return *cfg.Compression
}

func (cfg CompressionConfig) WALCodec() string {
	return codecOrNone(cfg.WAL)
}

func (cfg CompressionConfig) DumpCodec() string {
	return codecOrNone(cfg.Dump)
}

func (cfg CompressionConfig) ReplicationCodec() string {
	return codecOrNone(cfg.Replication)
}

func (cfg CompressionConfig) MinFrameSizeValue() int {
	if cfg.MinFrameSize <= 0 {
		return DefaultCompressionMinFrameSize
	}

	return cfg.MinFrameSize
}

func codecOrNone(value string) string {
	if value == "" {
		return "none"
	}

	return value
}

//nolint:tagliatelle // it's ok
type ReplicationConfig struct {
	ReplicaType   string                 `yaml:"replica_type"`
	ReplicaID     string                 `yaml:"replica_id"`
	MasterAddress string                 `yaml:"master_address"`
	SyncInterval  time.Duration          `yaml:"sync_interval"`
	Auth          *ReplicationAuthConfig `yaml:"auth"`
	TLS           TLSConfig              `yaml:"tls"`
}

func Load(path string) (Config, error) {
	info, err := os.Stat(path)
	if err != nil {
		return Config{}, fmt.Errorf("stat config %q: %w", path, err)
	}

	if info.IsDir() {
		return Config{}, fmt.Errorf("config %q is a directory", path)
	}

	data, err := os.ReadFile(path)
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

	if err := resolveSecrets(&cfg); err != nil {
		return Config{}, fmt.Errorf("resolve secrets: %w", err)
	}

	return cfg, nil
}

func resolveSecrets(cfg *Config) error {
	seen := make(map[security.Secret]struct{}, len(cfg.Network.Auth.Tokens))

	for i := range cfg.Network.Auth.Tokens {
		token := &cfg.Network.Auth.Tokens[i]

		role, err := security.ParseRole(token.Role)
		if err != nil {
			return fmt.Errorf("network auth token %d: %w", i, err)
		}

		secret, err := security.LoadSecret(token.TokenEnv, token.TokenFile)
		if err != nil {
			return fmt.Errorf("network auth token %d: %w", i, err)
		}

		if _, found := seen[secret]; found {
			return fmt.Errorf("network auth token %d: %w", i, security.ErrDuplicateToken)
		}

		seen[secret] = struct{}{}
		token.role = role
		token.secret = secret
	}

	if cfg.Replication.Auth != nil {
		secret, err := security.LoadSecret(cfg.Replication.Auth.TokenEnv, cfg.Replication.Auth.TokenFile)
		if err != nil {
			return fmt.Errorf("replication auth: %w", err)
		}

		cfg.Replication.Auth.secret = secret
	}

	return nil
}

func validateAuthConfig(cfg AuthConfig) error {
	for i := range cfg.Tokens {
		token := cfg.Tokens[i]

		if _, err := security.ParseRole(token.Role); err != nil {
			return fmt.Errorf("token %d: %w", i, err)
		}

		if (token.TokenEnv == "") == (token.TokenFile == "") {
			return fmt.Errorf("token %d: %w", i, security.ErrSecretSourceAmbiguous)
		}
	}

	return nil
}

func validateTLSConfig(cfg TLSConfig) error {
	if (cfg.CertFile == "") != (cfg.KeyFile == "") {
		return security.ErrTLSKeyPairIncomplete
	}

	switch cfg.MinVersion {
	case "", "1.2", "1.3":
		return nil
	default:
		return fmt.Errorf("%w: %q", security.ErrTLSUnknownMinVersion, cfg.MinVersion)
	}
}

func validateReplicationAuthConfig(cfg *ReplicationAuthConfig) error {
	if cfg == nil {
		return security.ErrSecretSourceAmbiguous
	}

	if (cfg.TokenEnv == "") == (cfg.TokenFile == "") {
		return security.ErrSecretSourceAmbiguous
	}

	return nil
}

func validateReplicationTLSConfig(replicaType string, cfg TLSConfig) error {
	if err := validateTLSConfig(cfg); err != nil {
		return err
	}

	if replicaType == ReplicaTypeMaster &&
		!cfg.Options().Empty() &&
		(cfg.CertFile == "" || cfg.KeyFile == "") {
		return security.ErrTLSCertRequired
	}

	return nil
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
		validation.Field(&cfg.Engine.Partitions, validation.Min(0)),
		validation.Field(&cfg.Engine.WALApplyWorkers, validation.Min(0)),
		validation.Field(&cfg.Engine.LimitEventQueueCapacity, validation.Min(0)),
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

	if err := validateAuthConfig(cfg.Network.Auth); err != nil {
		return fmt.Errorf("validate network auth section: %w", err)
	}

	if err := validateTLSConfig(cfg.Network.TLS); err != nil {
		return fmt.Errorf("validate network tls section: %w", err)
	}

	err = validation.ValidateStruct(&cfg.Observability,
		validation.Field(&cfg.Observability.Address, addressIfSetRule),
	)
	if err != nil {
		return fmt.Errorf("validate observability section: %w", err)
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
		validation.Field(&cfg.Replication.ReplicaID, validation.When(
			cfg.Replication.ReplicaType == ReplicaTypeSlave,
			validation.Required,
		)),
		validation.Field(&cfg.Replication.MasterAddress, addressIfSetRule),
		validation.Field(&cfg.Replication.SyncInterval, nonNegativeDurationRule),
	)
	if err != nil {
		return fmt.Errorf("validate replication section: %w", err)
	}

	if cfg.Replication.ReplicaType != "" {
		if err := validateReplicationAuthConfig(cfg.Replication.Auth); err != nil {
			return fmt.Errorf("validate replication auth section: %w", err)
		}
	}

	if err := validateReplicationTLSConfig(cfg.Replication.ReplicaType, cfg.Replication.TLS); err != nil {
		return fmt.Errorf("validate replication tls section: %w", err)
	}

	if cfg.Compression != nil {
		err = validation.ValidateStruct(cfg.Compression,
			validation.Field(&cfg.Compression.WAL, validation.In("", "none", "s2", "zstd")),
			validation.Field(&cfg.Compression.Dump, validation.In("", "none", "s2", "zstd")),
			validation.Field(&cfg.Compression.Replication, validation.In("", "none", "s2", "zstd")),
			validation.Field(&cfg.Compression.MinFrameSize, validation.Min(0)),
		)
		if err != nil {
			return fmt.Errorf("validate compression section: %w", err)
		}
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
		validation.Field(&cfg.QueueCapacity, validation.Min(0)),
		validation.Field(&cfg.MaxSegmentSize, validation.Required, sizeRule),
		validation.Field(&cfg.DataDirectory, validation.Required),
		validation.Field(&cfg.SyncCommit, validation.Required, validation.In(WALSyncCommitOn, WALSyncCommitOff)),
	)
	if err != nil {
		return fmt.Errorf("validate wal section: %w", err)
	}

	if cfg.QueueCapacity != 0 && cfg.QueueCapacity < cfg.FlushingBatchLength {
		return fmt.Errorf(
			"validate wal section: queue_capacity must be greater than or equal to flushing_batch_length",
		)
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
