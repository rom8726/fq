package replication

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog"
)

type TCPServer interface {
	Start(context.Context, func(context.Context, []byte) ([]byte, error)) error
}

type Master struct {
	server       TCPServer
	walDirectory string
	dumpProvider DumpProvider
	logger       *zerolog.Logger
}

func NewMaster(
	server TCPServer,
	walDirectory string,
	dumpProvider DumpProvider,
	logger *zerolog.Logger,
) (*Master, error) {
	if server == nil {
		return nil, errors.New("server is invalid")
	}

	if logger == nil {
		return nil, errors.New("logger is invalid")
	}

	return &Master{
		server:       server,
		walDirectory: walDirectory,
		dumpProvider: dumpProvider,
		logger:       logger,
	}, nil
}

func (m *Master) IsMaster() bool {
	return true
}

func (m *Master) Start(ctx context.Context) error {
	return m.server.Start(ctx, func(ctx context.Context, requestData []byte) ([]byte, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		var request Request
		if err := Decode(&request, requestData); err != nil {
			return nil, fmt.Errorf("failed to decode replication request: %w", err)
		}

		if request.DumpRequest.SessionUUID != "" {
			return m.processDump(request.DumpRequest), nil
		}

		return m.processWAL(request.WALRequest), nil
	})
}
