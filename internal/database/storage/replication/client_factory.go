package replication

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/fq-db/fq/internal/network"
)

// TCPClientFactoryImpl implements TCPClientFactory for creating TCP clients
type TCPClientFactoryImpl struct {
	address        string
	maxMessageSize int
	idleTimeout    time.Duration
	tlsConfig      *tls.Config
}

// NewTCPClientFactory creates a new TCP client factory
func NewTCPClientFactory(
	address string,
	maxMessageSize int,
	idleTimeout time.Duration,
	tlsConfig *tls.Config,
) *TCPClientFactoryImpl {
	return &TCPClientFactoryImpl{
		address:        address,
		maxMessageSize: maxMessageSize,
		idleTimeout:    idleTimeout,
		tlsConfig:      tlsConfig,
	}
}

// Create creates a new TCP client
func (f *TCPClientFactoryImpl) Create() (TCPClient, error) {
	var options []network.ClientOption
	if f.tlsConfig != nil {
		options = append(options, network.WithClientTLS(f.tlsConfig))
	}

	client, err := network.NewTCPClient(f.address, f.maxMessageSize, f.idleTimeout, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create TCP client: %w", err)
	}
	return client, nil
}
