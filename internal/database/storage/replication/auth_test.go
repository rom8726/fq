package replication

import (
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/protocol"
	"github.com/fq-db/fq/internal/security"
)

type capturingServer struct {
	handler func(context.Context, []byte) ([]byte, error)
}

func (s *capturingServer) Start(_ context.Context, handler func(context.Context, []byte) ([]byte, error)) error {
	s.handler = handler

	return nil
}

func newTestMaster(t *testing.T, secret security.Secret) *capturingServer {
	t.Helper()

	logger := zerolog.Nop()
	server := &capturingServer{}

	master, err := NewMaster(server, t.TempDir(), nil, secret, &logger)
	require.NoError(t, err)
	require.NoError(t, master.Start(context.Background()))
	require.NotNil(t, server.handler)

	return server
}

func TestMasterRejectsWrongToken(t *testing.T) {
	server := newTestMaster(t, security.Secret("replication-token-value"))

	request := NewWALRequest("wrong-token-value", "replica-1", "", 0, 0, nil)
	data, err := Encode(&request)
	require.NoError(t, err)

	responseData, err := server.handler(context.Background(), data)
	require.NoError(t, err)
	requireWALRejectionCode(t, responseData, protocol.CodeAuthenticationFailed)
}

func TestMasterRejectsMissingToken(t *testing.T) {
	server := newTestMaster(t, security.Secret("replication-token-value"))

	request := Request{WALRequest: WALRequest{ReplicaID: "replica-1"}}
	data, err := Encode(&request)
	require.NoError(t, err)

	responseData, err := server.handler(context.Background(), data)
	require.NoError(t, err)
	requireWALRejectionCode(t, responseData, protocol.CodeAuthenticationFailed)
}

func TestMasterRejectsDumpRequestWithWrongToken(t *testing.T) {
	server := newTestMaster(t, security.Secret("replication-token-value"))

	request := NewDumpRequest("wrong-token-value", "session-uuid", 0, nil)
	data, err := Encode(&request)
	require.NoError(t, err)

	responseData, err := server.handler(context.Background(), data)
	require.NoError(t, err)
	requireDumpRejectionCode(t, responseData, protocol.CodeAuthenticationFailed)
}

func TestMasterAcceptsCorrectToken(t *testing.T) {
	server := newTestMaster(t, security.Secret("replication-token-value"))

	request := NewWALRequest("replication-token-value", "replica-1", "", 0, 0, nil)
	data, err := Encode(&request)
	require.NoError(t, err)

	_, err = server.handler(context.Background(), data)
	require.NoError(t, err)
}

func TestMasterRejectsEverythingWhenSecretIsEmpty(t *testing.T) {
	server := newTestMaster(t, "")

	request := NewWALRequest("", "replica-1", "", 0, 0, nil)
	data, err := Encode(&request)
	require.NoError(t, err)

	responseData, err := server.handler(context.Background(), data)
	require.NoError(t, err)
	requireWALRejectionCode(t, responseData, protocol.CodeAuthenticationFailed)
}

func TestRequestConstructorsCarryToken(t *testing.T) {
	walRequest := NewWALRequest("token-value", "replica-1", "segment", 7, 9, nil)
	require.Equal(t, "token-value", walRequest.AuthToken)
	require.Equal(t, "replica-1", walRequest.ReplicaID)
	require.Equal(t, "segment", walRequest.LastSegmentName)

	dumpRequest := NewDumpRequest("token-value", "session-uuid", 3, nil)
	require.Equal(t, "token-value", dumpRequest.AuthToken)
	require.Equal(t, "session-uuid", dumpRequest.SessionUUID)
	require.Equal(t, uint64(3), dumpRequest.LastSegmentNumber)
}

func requireWALRejectionCode(t *testing.T, responseData []byte, code protocol.Code) {
	t.Helper()

	var response WALResponse
	require.NoError(t, Decode(&response, responseData))
	require.False(t, response.Succeed)
	require.Equal(t, code, response.ErrorCode)
}

func requireDumpRejectionCode(t *testing.T, responseData []byte, code protocol.Code) {
	t.Helper()

	var response DumpResponse
	require.NoError(t, Decode(&response, responseData))
	require.False(t, response.Succeed)
	require.Equal(t, code, response.ErrorCode)
}
