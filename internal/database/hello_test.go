package database

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/protocol"
	"github.com/fq-db/fq/internal/security"
)

func handshakeContext(t *testing.T) (context.Context, *protocol.Session) {
	t.Helper()

	session := protocol.NewSession()

	return protocol.WithSession(context.Background(), session), session
}

func authHandshakeContext(t *testing.T) (context.Context, *protocol.Session) {
	t.Helper()

	authCtx, _ := authContext(t)
	session := protocol.NewSession()

	return protocol.WithSession(authCtx, session), session
}

func TestHelloReturnsServerInfo(t *testing.T) {
	db := newTestDatabase(t)
	ctx, _ := handshakeContext(t)

	require.Equal(t, "ok|1;4096;0;admin", db.HandleQuery(ctx, "HELLO 1"))
}

func TestCommandBeforeHelloIsRejected(t *testing.T) {
	db := newTestDatabase(t)
	ctx, _ := handshakeContext(t)

	require.Equal(t, "err|1010|handshake required", db.HandleQuery(ctx, "GET key 60"))
	require.Equal(t, "err|1010|handshake required", db.HandleQuery(ctx, "AUTH sometoken"))
}

func TestHelloRejectsUnsupportedVersion(t *testing.T) {
	db := newTestDatabase(t)
	ctx, session := handshakeContext(t)

	require.Equal(t, "err|1011|unsupported protocol version: 7", db.HandleQuery(ctx, "HELLO 7"))
	require.False(t, session.Negotiated())
}

func TestHelloRejectsNonNumericVersion(t *testing.T) {
	db := newTestDatabase(t)
	ctx, _ := handshakeContext(t)

	require.Equal(t, "err|1002|invalid arguments", db.HandleQuery(ctx, "HELLO one"))
}

func TestHelloWithSameVersionIsIdempotent(t *testing.T) {
	db := newTestDatabase(t)
	ctx, _ := handshakeContext(t)

	require.Equal(t, "ok|1;4096;0;admin", db.HandleQuery(ctx, "HELLO 1"))
	require.Equal(t, "ok|1;4096;0;admin", db.HandleQuery(ctx, "HELLO 1"))
}

func TestHelloWithChangedVersionIsRejected(t *testing.T) {
	db := newTestDatabase(t)
	ctx, _ := handshakeContext(t)

	require.Equal(t, "ok|1;4096;0;admin", db.HandleQuery(ctx, "HELLO 1"))
	require.Equal(t, "err|1012|protocol version already negotiated", db.HandleQuery(ctx, "HELLO 2"))
}

func TestHelloWithInlineAuthAssignsRole(t *testing.T) {
	db := newTestDatabase(t)
	ctx, _ := authHandshakeContext(t)

	require.Equal(t, "ok|1;4096;1;rw", db.HandleQuery(ctx, "HELLO 1 AUTH rw-token-value"))
}

func TestHelloWithoutAuthReportsNoRole(t *testing.T) {
	db := newTestDatabase(t)
	ctx, _ := authHandshakeContext(t)

	require.Equal(t, "ok|1;4096;1;none", db.HandleQuery(ctx, "HELLO 1"))
}

func TestFailedInlineAuthStillNegotiatesVersion(t *testing.T) {
	db := newTestDatabase(t)
	ctx, session := authHandshakeContext(t)

	require.Equal(t, "err|3002|authentication failed", db.HandleQuery(ctx, "HELLO 1 AUTH wrong-token-value"))
	require.True(t, session.Negotiated())
	require.Equal(t, "err|3000|not authenticated", db.HandleQuery(ctx, "GET key 60"))
	require.Equal(t, "ok|1", db.HandleQuery(ctx, "AUTH rw-token-value"))
}

func TestTooManyInlineAuthFailuresWritesProtocolError(t *testing.T) {
	db := newTestDatabase(t)
	ctx, session := authHandshakeContext(t)

	for i := 1; i < security.MaxAuthFailures; i++ {
		require.Contains(t, db.HandleQuery(ctx, "HELLO 1 AUTH wrong-token-value"), "authentication failed")
	}

	var response []byte
	err := db.HandleQueryStream(ctx, "HELLO 1 AUTH wrong-token-value", func(msg []byte) error {
		response = append(response[:0], msg...)

		return nil
	})

	require.ErrorIs(t, err, security.ErrTooManyAuthFailures)
	require.Equal(t, "err|3003|too many authentication failures", string(response))
	require.True(t, session.Negotiated())
}
