package replication

import (
	"fmt"
	"time"

	"fq/internal/network"
)

// TCPClientFactoryImpl implements TCPClientFactory for creating TCP clients
type TCPClientFactoryImpl struct {
	address        string
	maxMessageSize int
	idleTimeout    time.Duration
}

// NewTCPClientFactory creates a new TCP client factory
func NewTCPClientFactory(address string, maxMessageSize int, idleTimeout time.Duration) *TCPClientFactoryImpl {
	return &TCPClientFactoryImpl{
		address:        address,
		maxMessageSize: maxMessageSize,
		idleTimeout:    idleTimeout,
	}
}

// Create creates a new TCP client
func (f *TCPClientFactoryImpl) Create() (TCPClient, error) {
	client, err := network.NewTCPClient(f.address, f.maxMessageSize, f.idleTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to create TCP client: %w", err)
	}
	return client, nil
}
