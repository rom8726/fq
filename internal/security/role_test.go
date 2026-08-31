package security_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/security"
)

func TestParseRole(t *testing.T) {
	tests := []struct {
		input string
		want  security.Role
	}{
		{"ro", security.RoleRO},
		{"RW", security.RoleRW},
		{"  admin  ", security.RoleAdmin},
	}

	for _, test := range tests {
		role, err := security.ParseRole(test.input)
		require.NoError(t, err)
		require.Equal(t, test.want, role)
	}
}

func TestParseRoleRejectsUnknown(t *testing.T) {
	_, err := security.ParseRole("root")
	require.ErrorIs(t, err, security.ErrUnknownRole)

	_, err = security.ParseRole("")
	require.ErrorIs(t, err, security.ErrUnknownRole)
}

func TestRoleString(t *testing.T) {
	require.Equal(t, "ro", security.RoleRO.String())
	require.Equal(t, "rw", security.RoleRW.String())
	require.Equal(t, "admin", security.RoleAdmin.String())
	require.Equal(t, "none", security.RoleNone.String())
}

func TestRoleAllows(t *testing.T) {
	require.True(t, security.RoleAdmin.Allows(security.RoleRO))
	require.True(t, security.RoleAdmin.Allows(security.RoleAdmin))
	require.True(t, security.RoleRW.Allows(security.RoleRO))
	require.False(t, security.RoleRW.Allows(security.RoleAdmin))
	require.False(t, security.RoleRO.Allows(security.RoleRW))
	require.False(t, security.RoleNone.Allows(security.RoleRO))
	require.False(t, security.RoleNone.Allows(security.RoleNone))
}
