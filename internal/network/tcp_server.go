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

var errFrameTooLarge = errors.New("frame exceeds maximum message size")

type TCPHandler = func(context.Context, []byte) ([]byte, error)
type TCPStreamHandler = func(context.Context, []byte, func([]byte) error) error

type TCPServer struct {
	address     string
	semaphore   tools.Semaphore
	idleTimeout time.Duration
	messageSize int
	logger      *zerolog.Logger
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

		request, err := readFrame(connection, s.messageSize)
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

		requestCtx, requestCancel := context.WithTimeout(ctx, s.idleTimeout)
		err = handler(requestCtx, request, func(response []byte) error {
			if len(response) > s.messageSize {
				s.logger.Error().
					Int("response_size", len(response)).
					Int("max_size", s.messageSize).
					Msg("handler response exceeds maximum, closing connection")

				return errFrameTooLarge
			}

			if err := writeFrame(connection, response); err != nil {
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

func readFrame(conn net.Conn, maxMessageSize int) ([]byte, error) {
	header := make([]byte, frameHeaderSize)
	messageSize, err := readFrameSize(conn, header, maxMessageSize)
	if err != nil {
		return nil, err
	}

	message := make([]byte, messageSize)
	if _, err := io.ReadFull(conn, message); err != nil {
		return nil, err
	}

	return message, nil
}

func readFrameInto(conn net.Conn, maxMessageSize int, buffer []byte) ([]byte, error) {
	header := make([]byte, frameHeaderSize)
	messageSize, err := readFrameSize(conn, header, maxMessageSize)
	if err != nil {
		return nil, err
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

func writeFrame(conn net.Conn, payload []byte) error {
	if uint64(len(payload)) > maxFramePayloadSize {
		return fmt.Errorf("%w: %d > %d", errFrameTooLarge, len(payload), maxFramePayloadSize)
	}

	header := make([]byte, frameHeaderSize)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))

	if err := writeAll(conn, header); err != nil {
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
