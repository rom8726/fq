package protocol_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/protocol"
)

func TestSessionNegotiatesCurrentVersion(t *testing.T) {
	session := protocol.NewSession()
	require.False(t, session.Negotiated())

	require.NoError(t, session.Negotiate(protocol.CurrentVersion))
	require.True(t, session.Negotiated())
	require.Equal(t, protocol.CurrentVersion, session.Version())
}

func TestSessionRejectsUnsupportedVersion(t *testing.T) {
	session := protocol.NewSession()

	err := session.Negotiate(7)

	code, ok := protocol.CodeOf(err)
	require.True(t, ok)
	require.Equal(t, protocol.CodeUnsupportedVersion, code)
	require.EqualError(t, err, "unsupported protocol version: 7")
	require.False(t, session.Negotiated())
}

func TestSessionRepeatedNegotiationIsIdempotent(t *testing.T) {
	session := protocol.NewSession()
	require.NoError(t, session.Negotiate(protocol.CurrentVersion))
	require.NoError(t, session.Negotiate(protocol.CurrentVersion))
}

func TestSessionRejectsVersionChange(t *testing.T) {
	session := protocol.NewSession()
	require.NoError(t, session.Negotiate(protocol.CurrentVersion))

	err := session.Negotiate(2)
	require.ErrorIs(t, err, protocol.ErrVersionAlreadyNegotiated)
}

func TestNilSessionNeedsNoHandshake(t *testing.T) {
	var session *protocol.Session
	require.True(t, session.Negotiated())
	require.Equal(t, uint16(0), session.Version())
}

func TestSessionRoundTripsThroughContext(t *testing.T) {
	session := protocol.NewSession()
	ctx := protocol.WithSession(context.Background(), session)

	require.Same(t, session, protocol.SessionFrom(ctx))
	require.Nil(t, protocol.SessionFrom(context.Background()))
}
