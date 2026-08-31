package replication

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"

	"github.com/rs/zerolog"

	"github.com/fq-db/fq/internal/observability"
	"github.com/fq-db/fq/internal/security"
)

var errReplicationUnauthorized = errors.New("replication request is not authorized")

const authFailurePort = "replication"

type TCPServer interface {
	Start(context.Context, func(context.Context, []byte) ([]byte, error)) error
}

type Master struct {
	server       TCPServer
	walDirectory string
	dumpProvider DumpProvider
	tracker      *ReplicaTracker
	secret       security.Secret
	logger       *zerolog.Logger
}

func NewMaster(
	server TCPServer,
	walDirectory string,
	dumpProvider DumpProvider,
	secret security.Secret,
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
		tracker:      NewReplicaTracker(),
		secret:       secret,
		logger:       logger,
	}, nil
}

func (m *Master) authorize(token string) bool {
	if m.secret.Empty() {
		return false
	}

	return subtle.ConstantTimeCompare([]byte(token), []byte(m.secret.Reveal())) == 1
}

func (m *Master) ReplicaCursors() []ReplicaCursor {
	if m.tracker == nil {
		return nil
	}

	return m.tracker.List()
}

func (m *Master) MinReplicaAckLSN() (uint64, bool) {
	if m.tracker == nil {
		return 0, false
	}

	return m.tracker.MinLastAppliedLSN()
}

func (m *Master) IsMaster() bool {
	return true
}

func (m *Master) Start(ctx context.Context) error {
	return m.server.Start(ctx, func(ctx context.Context, requestData []byte) ([]byte, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// Check if request data is empty or too short
		if len(requestData) == 0 {
			return nil, fmt.Errorf("empty replication request")
		}

		var request Request
		if err := Decode(&request, requestData); err != nil {
			m.logger.Warn().
				Err(err).
				Int("request_size", len(requestData)).
				Msg("failed to decode replication request, connection may be closing")
			return nil, fmt.Errorf("failed to decode replication request: %w", err)
		}

		if !m.authorize(request.AuthToken) {
			observability.IncAuthFailures(authFailurePort)

			m.logger.Warn().Msg("replication request rejected: invalid auth token")

			return nil, errReplicationUnauthorized
		}

		if request.SessionUUID != "" {
			return m.processDump(request.DumpRequest), nil
		}

		return m.processWAL(request.WALRequest), nil
	})
}
