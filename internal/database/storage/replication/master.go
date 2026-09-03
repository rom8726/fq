package replication

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"

	"github.com/rs/zerolog"

	"github.com/fq-db/fq/internal/database/storage/format"
	"github.com/fq-db/fq/internal/observability"
	"github.com/fq-db/fq/internal/protocol"
	"github.com/fq-db/fq/internal/security"
)

const authFailurePort = "replication"

type TCPServer interface {
	Start(context.Context, func(context.Context, []byte) ([]byte, error)) error
}

type Compression struct {
	SegmentCodec format.CodecID
	WireCodec    format.CodecID
	MinFrameSize int
}

type Master struct {
	server       TCPServer
	walDirectory string
	dumpProvider DumpProvider
	tracker      *ReplicaTracker
	secret       security.Secret
	compression  Compression
	logger       *zerolog.Logger
}

func NewMaster(
	server TCPServer,
	walDirectory string,
	dumpProvider DumpProvider,
	secret security.Secret,
	compression Compression,
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
		compression:  compression,
		logger:       logger,
	}, nil
}

func (m *Master) rejectRequest(request Request, code protocol.Code) []byte {
	var response any
	if request.SessionUUID != "" {
		response = &DumpResponse{ErrorCode: code}
	} else {
		response = &WALResponse{ErrorCode: code}
	}

	var responseData []byte
	var err error
	switch typed := response.(type) {
	case *DumpResponse:
		responseData, err = Encode(typed)
	case *WALResponse:
		responseData, err = Encode(typed)
	}
	if err != nil {
		m.logger.Error().Err(err).Uint16("error_code", uint16(code)).Msg("failed to encode rejection response")
	}

	return responseData
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

			return m.rejectRequest(request, protocol.CodeAuthenticationFailed), nil
		}

		if request.ProtocolVersion != ProtocolVersion {
			m.logger.Warn().
				Uint32("requested", request.ProtocolVersion).
				Uint32("supported", ProtocolVersion).
				Msg("replication request rejected: unsupported protocol version")

			return m.rejectRequest(request, protocol.CodeUnsupportedVersion), nil
		}

		if request.SessionUUID != "" {
			return m.processDump(request.DumpRequest, request.Codecs), nil
		}

		return m.processWAL(request.WALRequest, request.Codecs), nil
	})
}
