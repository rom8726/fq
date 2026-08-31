package initialization

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/rs/zerolog"

	"github.com/fq-db/fq/internal/config"
	"github.com/fq-db/fq/internal/network"
	"github.com/fq-db/fq/internal/security"
	"github.com/fq-db/fq/internal/tools"
)

func BuildRegistry(cfg config.AuthConfig) (*security.Registry, error) {
	registry := security.NewRegistry()

	for i := range cfg.Tokens {
		token := cfg.Tokens[i]
		if err := registry.Add(token.ResolvedSecret().Reveal(), token.ResolvedRole()); err != nil {
			return nil, fmt.Errorf("network auth token %d: %w", i, err)
		}
	}

	return registry, nil
}

const defaultServerAddress = "localhost:1945"
const defaultMaxConnectionNumber = 100
const defaultMaxMessageSize = 4096
const defaultIdleTimeout = time.Minute * 5

func CreateNetwork(
	cfg config.NetworkConfig,
	registry *security.Registry,
	logger *zerolog.Logger,
	tlsOptionsOverride ...security.TLSOptions,
) (*network.TCPServer, error) {
	address := defaultServerAddress
	maxConnectionsNumber := defaultMaxConnectionNumber
	maxMessageSize := defaultMaxMessageSize
	idleTimeout := defaultIdleTimeout

	if cfg.Address != "" {
		address = cfg.Address
	}

	if cfg.MaxConnections != 0 {
		maxConnectionsNumber = cfg.MaxConnections
	}

	if cfg.MaxMessageSize != "" {
		size, err := tools.ParseSize(cfg.MaxMessageSize)
		if err != nil {
			return nil, errors.New("incorrect max message size")
		}

		maxMessageSize = size
	}

	if cfg.IdleTimeout != 0 {
		idleTimeout = cfg.IdleTimeout
	}

	options := []network.ServerOption{
		network.WithConnContext(func(ctx context.Context, _ net.Conn) context.Context {
			return security.WithSession(ctx, security.NewSession(registry))
		}),
	}

	tlsOptions := cfg.TLS.Options()
	if len(tlsOptionsOverride) > 0 {
		tlsOptions = tlsOptionsOverride[0]
	}

	tlsConfig, err := tlsOptions.ServerConfig()
	if err != nil {
		return nil, fmt.Errorf("network tls: %w", err)
	}

	if tlsConfig != nil {
		options = append(options, network.WithTLS(tlsConfig))
	}

	if !registry.Enabled() {
		logger.Warn().Str("address", address).Msg("client port has no authentication configured")
	}

	return network.NewTCPServer(address, maxConnectionsNumber, maxMessageSize, idleTimeout, logger, options...)
}
