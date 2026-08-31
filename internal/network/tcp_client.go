package network

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/fq-db/fq/internal/protocol"
)

var ErrIdleTimeout = errors.New("idle timeout")

type TCPClient struct {
	connection     net.Conn
	frames         frameBuffer
	maxMessageSize int
	idleTimeout    time.Duration
	bufferPool     *bytesPool
}

func NewTCPClient(
	address string,
	maxMessageSize int,
	idleTimeout time.Duration,
	options ...ClientOption,
) (*TCPClient, error) {
	if maxMessageSize <= 0 {
		return nil, fmt.Errorf("invalid max message size: %d", maxMessageSize)
	}

	if uint64(maxMessageSize) > maxFramePayloadSize {
		return nil, fmt.Errorf("max message size exceeds frame limit: %d", maxMessageSize)
	}

	settings := clientOptions{}
	for _, option := range options {
		option(&settings)
	}

	connection, err := dial(address, settings.tlsConfig)
	if err != nil {
		return nil, err
	}

	return &TCPClient{
		connection:     connection,
		maxMessageSize: maxMessageSize,
		idleTimeout:    idleTimeout,
		bufferPool:     newBytesPool(maxMessageSize),
	}, nil
}

func dial(address string, tlsConfig *tls.Config) (net.Conn, error) {
	if tlsConfig == nil {
		connection, err := net.Dial("tcp", address)
		if err != nil {
			return nil, fmt.Errorf("failed to dial: %w", err)
		}

		return connection, nil
	}

	connection, err := tls.Dial("tcp", address, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to dial tls: %w", err)
	}

	return connection, nil
}

func (c *TCPClient) Send(ctx context.Context, request []byte) ([]byte, error) {
	var result []byte
	err := c.Stream(ctx, request, func(kind protocol.Kind, body []byte) error {
		if kind == protocol.KindNext {
			return protocol.ErrUnexpectedContinuation
		}

		result = make([]byte, len(body))
		copy(result, body)

		return io.EOF
	})
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return result, nil
}

func (c *TCPClient) Stream(ctx context.Context, request []byte, handle func(protocol.Kind, []byte) error) error {
	if len(request) > c.maxMessageSize {
		return fmt.Errorf("request exceeds max message size (%d)", c.maxMessageSize)
	}

	if err := c.connection.SetDeadline(c.deadline(ctx)); err != nil {
		return c.normalizeTimeoutError(ctx, err)
	}

	if err := c.frames.write(c.connection, request); err != nil {
		return c.normalizeTimeoutError(ctx, err)
	}

	response := c.bufferPool.Get()
	defer c.bufferPool.Put(response)

	for {
		if err := c.connection.SetDeadline(c.deadline(ctx)); err != nil {
			return c.normalizeTimeoutError(ctx, err)
		}

		message, err := c.frames.readInto(c.connection, c.maxMessageSize, response)
		if err != nil {
			return c.normalizeTimeoutError(ctx, err)
		}

		kind, body, parseErr := protocol.ParseResponse(message)
		if parseErr != nil {
			return parseErr
		}

		result := make([]byte, len(body))
		copy(result, body)

		if err := handle(kind, result); err != nil {
			return err
		}
	}
}

func (c *TCPClient) Hello(ctx context.Context, token string) (protocol.ServerInfo, error) {
	request := "HELLO " + strconv.FormatUint(uint64(protocol.CurrentVersion), 10)
	if token != "" {
		request += " AUTH " + token
	}

	body, err := c.Send(ctx, []byte(request))
	if err != nil {
		return protocol.ServerInfo{}, err
	}

	info, err := protocol.ParseServerInfo(body)
	if err != nil {
		return protocol.ServerInfo{}, err
	}

	c.SetMaxMessageSizeUnsafe(info.MaxMessageSize)

	return info, nil
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
