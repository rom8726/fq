package initialization

import (
	"errors"
	"time"

	"github.com/rs/zerolog"

	"fq/internal/config"
	"fq/internal/network"
	"fq/internal/tools"
)

const defaultServerAddress = "localhost:1945"
const defaultMaxConnectionNumber = 100
const defaultMaxMessageSize = 4096
const defaultIdleTimeout = time.Minute * 5

func CreateNetwork(cfg config.NetworkConfig, logger *zerolog.Logger) (*network.TCPServer, error) {
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

	return network.NewTCPServer(address, maxConnectionsNumber, maxMessageSize, idleTimeout, logger)
}
