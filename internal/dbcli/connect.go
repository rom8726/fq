package dbcli

import (
	"context"
	"fmt"
	"time"

	"github.com/fq-db/fq/internal/network"
	"github.com/fq-db/fq/internal/security"
)

type ConnectOptions struct {
	Address        string
	MaxMessageSize int
	IdleTimeout    time.Duration
	Token          string
	TLS            security.TLSOptions
}

func Connect(ctx context.Context, options ConnectOptions) (*network.TCPClient, error) {
	tlsConfig, err := options.TLS.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("build tls config: %w", err)
	}

	var clientOptions []network.ClientOption
	if tlsConfig != nil {
		clientOptions = append(clientOptions, network.WithClientTLS(tlsConfig))
	}

	client, err := network.NewTCPClient(
		options.Address, options.MaxMessageSize, options.IdleTimeout, clientOptions...,
	)
	if err != nil {
		return nil, err
	}

	if _, err := client.Hello(ctx, options.Token); err != nil {
		_ = client.Close()

		return nil, fmt.Errorf("handshake: %w", err)
	}

	return client, nil
}
