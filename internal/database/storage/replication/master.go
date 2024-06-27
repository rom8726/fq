package replication

import (
	"context"
	"errors"

	"github.com/rs/zerolog"
)

type TCPServer interface {
	Start(context.Context, func(context.Context, []byte) []byte) error
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
	return m.server.Start(ctx, func(ctx context.Context, requestData []byte) []byte {
		if ctx.Err() != nil {
			return nil
		}

		var request Request
		if err := Decode(&request, requestData); err != nil {
			m.logger.Error().Err(err).Msg("failed to decode replication request")

			return nil
		}

		if request.DumpRequest.SessionUUID != "" {
			return m.processDump(request.DumpRequest)
		}

		return m.processWAL(request.WALRequest)
	})
}
