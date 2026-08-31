package dbcli

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/fq-db/fq/internal/network"
	"github.com/fq-db/fq/internal/security"
)

const errorResponsePrefix = "err|"

var ErrAuthenticationRejected = errors.New("authentication rejected")

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

	if err := Authenticate(ctx, client, options.Token); err != nil {
		_ = client.Close()

		return nil, err
	}

	return client, nil
}

func Authenticate(ctx context.Context, client *network.TCPClient, token string) error {
	if token == "" {
		return nil
	}

	response, err := client.Send(ctx, []byte("AUTH "+token))
	if err != nil {
		return fmt.Errorf("send auth: %w", err)
	}

	if len(response) == 0 {
		return fmt.Errorf("%w: connection closed by server", ErrAuthenticationRejected)
	}

	if strings.HasPrefix(string(response), errorResponsePrefix) {
		return fmt.Errorf("%w: %s", ErrAuthenticationRejected, strings.TrimPrefix(string(response), errorResponsePrefix))
	}

	return nil
}
