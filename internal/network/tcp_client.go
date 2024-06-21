package network

import (
	"fmt"
	"net"
	"time"
)

type TCPClient struct {
	connection     net.Conn
	maxMessageSize int
	idleTimeout    time.Duration
	bufferPool     *bytesPool
}

func NewTCPClient(address string, maxMessageSize int, idleTimeout time.Duration) (*TCPClient, error) {
	connection, err := net.Dial("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	return &TCPClient{
		connection:     connection,
		maxMessageSize: maxMessageSize,
		idleTimeout:    idleTimeout,
		bufferPool:     newBytesPool(maxMessageSize),
	}, nil
}

func (c *TCPClient) Send(request []byte) ([]byte, error) {
	if len(request) > c.maxMessageSize {
		return nil, fmt.Errorf("request exceeds max message size (%d)", c.maxMessageSize)
	}

	if err := c.connection.SetDeadline(time.Now().Add(c.idleTimeout)); err != nil {
		return nil, err
	}

	if _, err := c.connection.Write(request); err != nil {
		return nil, err
	}

	response := c.bufferPool.Get()
	defer c.bufferPool.Put(response)

	count, err := c.connection.Read(response)
	if err != nil {
		return nil, err
	}

	result := make([]byte, count)
	copy(result, response[:count])

	return result, nil
}
