package security_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/security"
)

func TestRegistryEmptyIsDisabled(t *testing.T) {
	registry := security.NewRegistry()

	require.False(t, registry.Enabled())

	role, ok := registry.Authenticate("anything")
	require.False(t, ok)
	require.Equal(t, security.RoleNone, role)
}

func TestRegistryNilIsDisabled(t *testing.T) {
	var registry *security.Registry

	require.False(t, registry.Enabled())

	_, ok := registry.Authenticate("anything")
	require.False(t, ok)
}

func TestRegistryAuthenticatesEachRole(t *testing.T) {
	registry := security.NewRegistry()
	require.NoError(t, registry.Add("admin-token-value", security.RoleAdmin))
	require.NoError(t, registry.Add("rw-token-value", security.RoleRW))
	require.NoError(t, registry.Add("ro-token-value", security.RoleRO))

	require.True(t, registry.Enabled())

	role, ok := registry.Authenticate("admin-token-value")
	require.True(t, ok)
	require.Equal(t, security.RoleAdmin, role)

	role, ok = registry.Authenticate("rw-token-value")
	require.True(t, ok)
	require.Equal(t, security.RoleRW, role)

	role, ok = registry.Authenticate("ro-token-value")
	require.True(t, ok)
	require.Equal(t, security.RoleRO, role)
}

func TestRegistryRejectsUnknownToken(t *testing.T) {
	registry := security.NewRegistry()
	require.NoError(t, registry.Add("admin-token-value", security.RoleAdmin))

	role, ok := registry.Authenticate("wrong-token-value")
	require.False(t, ok)
	require.Equal(t, security.RoleNone, role)

	role, ok = registry.Authenticate("")
	require.False(t, ok)
	require.Equal(t, security.RoleNone, role)
}

func TestRegistryRejectsDuplicateToken(t *testing.T) {
	registry := security.NewRegistry()
	require.NoError(t, registry.Add("same-token-value", security.RoleAdmin))

	err := registry.Add("same-token-value", security.RoleRO)
	require.ErrorIs(t, err, security.ErrDuplicateToken)
}

func TestRegistryRejectsNoneRole(t *testing.T) {
	registry := security.NewRegistry()

	err := registry.Add("some-token-value", security.RoleNone)
	require.ErrorIs(t, err, security.ErrUnknownRole)
}
