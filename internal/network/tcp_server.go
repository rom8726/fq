package network

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/fq-db/fq/internal/observability"
	"github.com/fq-db/fq/internal/tools"
)

const (
	frameHeaderSize     = 4
	maxFramePayloadSize = 1<<32 - 1
)

var (
	errFrameTooLarge                    = errors.New("frame exceeds maximum message size")
	noCancel         context.CancelFunc = func() {}
)

type TCPHandler = func(context.Context, []byte) ([]byte, error)
type TCPStreamHandler = func(context.Context, []byte, func([]byte) error) error

type TCPServer struct {
	address     string
	semaphore   tools.Semaphore
	idleTimeout time.Duration
	messageSize int
	logger      *zerolog.Logger
}

type frameBuffer struct {
	header  [frameHeaderSize]byte
	payload []byte
}

func NewTCPServer(
	address string,
	maxConnectionsNumber int,
	maxMessageSize int,
	idleTimeout time.Duration,
	logger *zerolog.Logger,
) (*TCPServer, error) {
	if logger == nil {
		return nil, errors.New("logger is invalid")
	}

	if maxConnectionsNumber <= 0 {
		return nil, errors.New("invalid number of max connections")
	}

	if maxMessageSize <= 0 {
		return nil, errors.New("invalid max message size")
	}

	if uint64(maxMessageSize) > maxFramePayloadSize {
		return nil, errors.New("max message size exceeds frame limit")
	}

	return &TCPServer{
		address:     address,
		semaphore:   tools.NewSemaphore(maxConnectionsNumber),
		idleTimeout: idleTimeout,
		messageSize: maxMessageSize,
		logger:      logger,
	}, nil
}

func (s *TCPServer) HandleQueries(ctx context.Context, handler TCPHandler) error {
	return s.HandleQueryStreams(ctx, func(ctx context.Context, request []byte, write func([]byte) error) error {
		response, err := handler(ctx, request)
		if err != nil {
			return err
		}

		return write(response)
	})
}

func (s *TCPServer) HandleQueryStreams(ctx context.Context, handler TCPStreamHandler) error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		for {
			connection, err := listener.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return
				}

				s.logger.Error().Err(err).Msg("failed to accept")

				continue
			}

			s.logger.Info().Msg("accepted connection")

			wg.Add(1)
			go func(connection net.Conn) {
				s.semaphore.Acquire()
				observability.IncTCPActiveConnections()

				defer func() {
					observability.DecTCPActiveConnections()
					s.semaphore.Release()
					wg.Done()
				}()

				s.handleConnection(ctx, connection, handler)
			}(connection)
		}
	}()

	go func() {
		defer wg.Done()

		<-ctx.Done()
		if err := listener.Close(); err != nil {
			s.logger.Warn().Err(err).Msg("failed to close listener")
		}
	}()

	wg.Wait()

	return nil
}

func (s *TCPServer) Start(ctx context.Context, handler func(context.Context, []byte) ([]byte, error)) error {
	return s.HandleQueries(ctx, handler)
}

func (s *TCPServer) handleConnection(ctx context.Context, connection net.Conn, handler TCPStreamHandler) {
	stopClose := make(chan struct{})
	frames := frameBuffer{}
	go func() {
		select {
		case <-ctx.Done():
			_ = connection.Close()
		case <-stopClose:
		}
	}()
	defer close(stopClose)

	for {
		if err := connection.SetDeadline(time.Now().Add(s.idleTimeout)); err != nil {
			s.logger.Warn().Err(err).Msg("failed to set read deadline")

			break
		}

		request, err := frames.read(connection, s.messageSize)
		if err != nil {
			if errors.Is(err, errFrameTooLarge) {
				s.logger.Warn().
					Int("max_size", s.messageSize).
					Msg("message size exceeds maximum, closing connection")
			} else if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				s.logger.Warn().Err(err).Msg("failed to read")
			}

			break
		}

		requestCtx, requestCancel := s.requestContext(ctx, request)
		err = handler(requestCtx, request, func(response []byte) error {
			if len(response) > s.messageSize {
				s.logger.Error().
					Int("response_size", len(response)).
					Int("max_size", s.messageSize).
					Msg("handler response exceeds maximum, closing connection")

				return errFrameTooLarge
			}

			if err := frames.write(connection, response); err != nil {
				s.logger.Warn().Err(err).Msg("failed to write")

				return err
			}

			return nil
		})
		requestCancel()
		if err != nil {
			s.logger.Error().Err(err).Msg("handler failed")

			break
		}
	}

	s.logger.Warn().Msg("close connection")

	if err := connection.Close(); err != nil {
		s.logger.Warn().Err(err).Msg("failed to close connection")
	}
}

func (s *TCPServer) requestContext(ctx context.Context, request []byte) (context.Context, context.CancelFunc) {
	if requestNeedsTimeout(request) {
		return context.WithTimeout(ctx, s.idleTimeout)
	}

	return ctx, noCancel
}

func requestNeedsTimeout(request []byte) bool {
	command := firstToken(request)

	return tokenEquals(command, "WATCH") ||
		tokenEquals(command, "STREAM") ||
		tokenEquals(command, "PSTREAM")
}

func firstToken(request []byte) []byte {
	start := 0
	for start < len(request) && isTokenSpace(request[start]) {
		start++
	}

	end := start
	for end < len(request) && !isTokenSpace(request[end]) {
		end++
	}

	return request[start:end]
}

func isTokenSpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r'
}

func tokenEquals(token []byte, value string) bool {
	if len(token) != len(value) {
		return false
	}

	for i := range token {
		if token[i] != value[i] {
			return false
		}
	}

	return true
}

func (b *frameBuffer) read(conn net.Conn, maxMessageSize int) ([]byte, error) {
	messageSize, err := readFrameSize(conn, b.header[:], maxMessageSize)
	if err != nil {
		return nil, err
	}

	if cap(b.payload) < messageSize {
		b.payload = make([]byte, messageSize)
	}

	message := b.payload[:messageSize]
	if _, err := io.ReadFull(conn, message); err != nil {
		return nil, err
	}

	return message, nil
}

func readFrameInto(conn net.Conn, maxMessageSize int, buffer []byte) ([]byte, error) {
	var header [frameHeaderSize]byte
	messageSize, err := readFrameSize(conn, header[:], maxMessageSize)
	if err != nil {
		return nil, err
	}
	if messageSize > len(buffer) {
		return nil, fmt.Errorf("%w: %d > %d", errFrameTooLarge, messageSize, len(buffer))
	}

	message := buffer[:messageSize]
	if _, err := io.ReadFull(conn, message); err != nil {
		return nil, err
	}

	return message, nil
}

func readFrameSize(conn net.Conn, header []byte, maxMessageSize int) (int, error) {
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, err
	}

	messageSize := binary.BigEndian.Uint32(header)
	if messageSize > uint32(maxMessageSize) {
		return 0, fmt.Errorf("%w: %d > %d", errFrameTooLarge, messageSize, maxMessageSize)
	}

	return int(messageSize), nil
}

func (b *frameBuffer) write(conn net.Conn, payload []byte) error {
	return writeFrameWithHeader(conn, payload, b.header[:])
}

func writeFrameWithHeader(conn net.Conn, payload, header []byte) error {
	if uint64(len(payload)) > maxFramePayloadSize {
		return fmt.Errorf("%w: %d > %d", errFrameTooLarge, len(payload), maxFramePayloadSize)
	}
	if len(header) < frameHeaderSize {
		return fmt.Errorf("frame header buffer too small: %d < %d", len(header), frameHeaderSize)
	}

	binary.BigEndian.PutUint32(header, uint32(len(payload)))

	if err := writeAll(conn, header[:frameHeaderSize]); err != nil {
		return err
	}

	return writeAll(conn, payload)
}

func writeAll(conn net.Conn, data []byte) error {
	for len(data) > 0 {
		n, err := conn.Write(data)
		if err != nil {
			return err
		}

		if n == 0 {
			return io.ErrShortWrite
		}

		data = data[n:]
	}

	return nil
}
