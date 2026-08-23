package network

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
)

var ErrIdleTimeout = errors.New("idle timeout")

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
	var result []byte
	err := c.Stream(ctx, request, func(message []byte) error {
		result = make([]byte, len(message))
		copy(result, message)

		return io.EOF
	})
	if err != nil && err != io.EOF {
		return nil, err
	}

	return result, nil
}

func (c *TCPClient) Stream(ctx context.Context, request []byte, handle func([]byte) error) error {
	if len(request) > c.maxMessageSize {
		return fmt.Errorf("request exceeds max message size (%d)", c.maxMessageSize)
	}

	if err := c.connection.SetDeadline(c.deadline(ctx)); err != nil {
		return c.normalizeTimeoutError(ctx, err)
	}

	if err := writeFrame(c.connection, request); err != nil {
		return c.normalizeTimeoutError(ctx, err)
	}

	response := c.bufferPool.Get()
	defer c.bufferPool.Put(response)

	for {
		if err := c.connection.SetDeadline(c.deadline(ctx)); err != nil {
			return c.normalizeTimeoutError(ctx, err)
		}

		message, err := readFrameInto(c.connection, c.maxMessageSize, response)
		if err != nil {
			return c.normalizeTimeoutError(ctx, err)
		}

		result := make([]byte, len(message))
		copy(result, message)

		if err := handle(result); err != nil {
			return err
		}
	}
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

func (c *TCPClient) normalizeTimeoutError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}

	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ErrIdleTimeout
	}

	return err
}
