package security_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/protocol"
	"github.com/fq-db/fq/internal/security"
)

func newTestRegistry(t *testing.T) *security.Registry {
	t.Helper()

	registry := security.NewRegistry()
	require.NoError(t, registry.Add("admin-token-value", security.RoleAdmin))
	require.NoError(t, registry.Add("rw-token-value", security.RoleRW))
	require.NoError(t, registry.Add("ro-token-value", security.RoleRO))

	return registry
}

func TestSessionRejectsBeforeAuthentication(t *testing.T) {
	session := security.NewSession(newTestRegistry(t))

	require.True(t, session.Enabled())
	require.ErrorIs(t, session.Authorize(security.RoleRO), security.ErrNotAuthenticated)
}

func TestSessionAuthenticatesAndAuthorizes(t *testing.T) {
	session := security.NewSession(newTestRegistry(t))

	require.NoError(t, session.Authenticate("rw-token-value"))
	require.Equal(t, security.RoleRW, session.Role())
	require.NoError(t, session.Authorize(security.RoleRO))
	require.NoError(t, session.Authorize(security.RoleRW))
	require.ErrorIs(t, session.Authorize(security.RoleAdmin), security.ErrPermissionDenied)
}

func TestSessionCountsFailuresAndTripsLimit(t *testing.T) {
	session := security.NewSession(newTestRegistry(t))

	for i := 1; i < security.MaxAuthFailures; i++ {
		require.ErrorIs(t, session.Authenticate("bad-token-value"), security.ErrAuthenticationFailed)
		require.Equal(t, i, session.Failures())
	}

	require.ErrorIs(t, session.Authenticate("bad-token-value"), security.ErrTooManyAuthFailures)
	require.Equal(t, security.MaxAuthFailures, session.Failures())
}

func TestSessionResetsFailuresOnSuccess(t *testing.T) {
	session := security.NewSession(newTestRegistry(t))

	require.ErrorIs(t, session.Authenticate("bad-token-value"), security.ErrAuthenticationFailed)
	require.NoError(t, session.Authenticate("admin-token-value"))
	require.Zero(t, session.Failures())
	require.Equal(t, security.RoleAdmin, session.Role())
}

func TestDisabledSessionAllowsEverything(t *testing.T) {
	session := security.NewSession(security.NewRegistry())

	require.False(t, session.Enabled())
	require.NoError(t, session.Authorize(security.RoleAdmin))
	require.NoError(t, session.Authenticate("anything"))
	require.Equal(t, security.RoleAdmin, session.Role())
}

func TestNilSessionAllowsEverything(t *testing.T) {
	var session *security.Session

	require.False(t, session.Enabled())
	require.NoError(t, session.Authorize(security.RoleAdmin))
}

func TestSessionContextRoundTrip(t *testing.T) {
	session := security.NewSession(newTestRegistry(t))
	ctx := security.WithSession(context.Background(), session)

	require.Same(t, session, security.SessionFrom(ctx))
	require.Nil(t, security.SessionFrom(context.Background()))
}

func TestSecurityErrorsCarryCodes(t *testing.T) {
	cases := []struct {
		err  error
		code protocol.Code
	}{
		{security.ErrNotAuthenticated, protocol.CodeNotAuthenticated},
		{security.ErrPermissionDenied, protocol.CodePermissionDenied},
		{security.ErrAuthenticationFailed, protocol.CodeAuthenticationFailed},
		{security.ErrTooManyAuthFailures, protocol.CodeTooManyAuthFailures},
	}

	for _, tc := range cases {
		code, ok := protocol.CodeOf(tc.err)
		require.True(t, ok, tc.err.Error())
		require.Equal(t, tc.code, code, tc.err.Error())
	}
}
