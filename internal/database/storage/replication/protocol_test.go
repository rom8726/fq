package replication_test

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database/storage/replication"
	"github.com/fq-db/fq/internal/protocol"
)

func TestNewDumpRequestCarriesProtocolVersion(t *testing.T) {
	t.Parallel()

	request := replication.NewDumpRequest("token", "session-uuid", 7, nil)

	require.Equal(t, replication.ProtocolVersion, request.ProtocolVersion)
}

func TestNewWALRequestCarriesProtocolVersion(t *testing.T) {
	t.Parallel()

	request := replication.NewWALRequest("token", "replica-1", "0001.wal", 0, 0, nil)

	require.Equal(t, replication.ProtocolVersion, request.ProtocolVersion)
}

func TestLegacyRequestDecodesWithZeroVersion(t *testing.T) {
	t.Parallel()

	type legacyDumpRequest struct {
		SessionUUID       string
		LastSegmentNumber uint64
	}

	type legacyWALRequest struct {
		ReplicaID       string
		LastSegmentName string
		SegmentOffset   int64
		LastAppliedLSN  uint64
	}

	type legacyRequest struct {
		AuthToken string
		legacyDumpRequest
		legacyWALRequest
	}

	var buffer bytes.Buffer
	legacy := legacyRequest{AuthToken: "token"}
	legacy.SessionUUID = "session-uuid"
	require.NoError(t, gob.NewEncoder(&buffer).Encode(&legacy))

	var request replication.Request
	require.NoError(t, replication.Decode(&request, buffer.Bytes()))
	require.Equal(t, uint32(0), request.ProtocolVersion)
	require.Equal(t, "token", request.AuthToken)
}

func TestSlaveRejectsUnsupportedVersionResponse(t *testing.T) {
	t.Parallel()

	response := replication.DumpResponse{ErrorCode: protocol.CodeUnsupportedVersion}

	data, err := replication.Encode(&response)
	require.NoError(t, err)

	var decoded replication.DumpResponse
	require.NoError(t, replication.Decode(&decoded, data))
	require.False(t, decoded.Succeed)
	require.Equal(t, protocol.CodeUnsupportedVersion, decoded.ErrorCode)
}
