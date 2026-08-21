package network

import (
	"context"
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
	if maxMessageSize <= 0 {
		return nil, fmt.Errorf("invalid max message size: %d", maxMessageSize)
	}

	if uint64(maxMessageSize) > maxFramePayloadSize {
		return nil, fmt.Errorf("max message size exceeds frame limit: %d", maxMessageSize)
	}

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

func (c *TCPClient) Send(ctx context.Context, request []byte) ([]byte, error) {
	if len(request) > c.maxMessageSize {
		return nil, fmt.Errorf("request exceeds max message size (%d)", c.maxMessageSize)
	}

	if err := c.connection.SetDeadline(c.deadline(ctx)); err != nil {
		return nil, err
	}

	if err := writeFrame(c.connection, request); err != nil {
		return nil, err
	}

	response := c.bufferPool.Get()
	defer c.bufferPool.Put(response)

	message, err := readFrameInto(c.connection, c.maxMessageSize, response)
	if err != nil {
		return nil, err
	}

	result := make([]byte, len(message))
	copy(result, message)

	return result, nil
}

func (c *TCPClient) Close() error {
	return c.connection.Close()
}

func (c *TCPClient) SetMaxMessageSizeUnsafe(size int) {
	c.maxMessageSize = size
	c.bufferPool = newBytesPool(size)
}

func (c *TCPClient) deadline(ctx context.Context) time.Time {
	stdDeadline := time.Now().Add(c.idleTimeout)
	deadline, ok := ctx.Deadline()
	if ok {
		if stdDeadline.Before(deadline) {
			deadline = stdDeadline
		}
	} else {
		deadline = stdDeadline
	}

	return deadline
}
