package network

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"fq/internal/tools"
)

type TCPHandler = func(context.Context, []byte) ([]byte, error)

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

	return &TCPServer{
		address:     address,
		semaphore:   tools.NewSemaphore(maxConnectionsNumber),
		idleTimeout: idleTimeout,
		messageSize: maxMessageSize,
		logger:      logger,
	}, nil
}

func (s *TCPServer) HandleQueries(ctx context.Context, handler TCPHandler) error {
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

				defer func() {
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

func (s *TCPServer) handleConnection(ctx context.Context, connection net.Conn, handler TCPHandler) {
	request := make([]byte, s.messageSize)

	for {
		if err := connection.SetDeadline(time.Now().Add(s.idleTimeout)); err != nil {
			s.logger.Warn().Err(err).Msg("failed to set read deadline")

			break
		}

		count, err := s.connRead(ctx, connection, request)
		if err != nil {
			if err != io.EOF {
				s.logger.Warn().Err(err).Msg("failed to read")
			}

			break
		}

		response, err := handler(ctx, request[:count])
		if err != nil {
			s.logger.Error().Err(err).Msg("handler failed")

			break
		}

		if _, err := connection.Write(response); err != nil {
			s.logger.Warn().Err(err).Msg("failed to write")

			break
		}
	}

	s.logger.Warn().Msg("close connection")

	if err := connection.Close(); err != nil {
		s.logger.Warn().Err(err).Msg("failed to close connection")
	}
}

func (s *TCPServer) connRead(ctx context.Context, conn net.Conn, buff []byte) (int, error) {
	type readResult struct {
		n   int
		err error
	}

	result := make(chan readResult)

	go func() {
		defer close(result)

		n, err := conn.Read(buff)
		result <- readResult{n: n, err: err}
	}()

	select {
	case <-ctx.Done():
		_ = conn.SetReadDeadline(time.Now())

		return 0, ctx.Err()
	case res := <-result:
		return res.n, nil
	}
}
