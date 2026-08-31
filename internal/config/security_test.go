package config

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/security"
)

func TestLoadResolvesNetworkTokensFromEnv(t *testing.T) {
	t.Setenv("FQ_TEST_ADMIN", "admin-token-value-1234")
	t.Setenv("FQ_TEST_RO", "ro-token-value-12345678")
	t.Setenv("FQ_TEST_REPLICATION", "replication-token-value")

	cfg := validConfig()
	cfg.Network.Auth = AuthConfig{
		Tokens: []TokenConfig{
			{Role: "admin", TokenEnv: "FQ_TEST_ADMIN"},
			{Role: "ro", TokenEnv: "FQ_TEST_RO"},
		},
	}

	require.NoError(t, resolveSecrets(&cfg))
	require.NoError(t, validate(&cfg))
	require.Equal(t, security.RoleAdmin, cfg.Network.Auth.Tokens[0].ResolvedRole())
	require.Equal(t, "admin-token-value-1234", cfg.Network.Auth.Tokens[0].ResolvedSecret().Reveal())
	require.Equal(t, security.RoleRO, cfg.Network.Auth.Tokens[1].ResolvedRole())
}

func TestLoadRejectsInvalidNetworkTokens(t *testing.T) {
	t.Setenv("FQ_TEST_ADMIN", "admin-token-value-1234")
	t.Setenv("FQ_TEST_DUP", "admin-token-value-1234")
	t.Setenv("FQ_TEST_SHORT", "short")
	t.Setenv("FQ_TEST_REPLICATION", "replication-token-value")

	tests := []struct {
		name   string
		tokens []TokenConfig
	}{
		{
			name:   "unknown role",
			tokens: []TokenConfig{{Role: "root", TokenEnv: "FQ_TEST_ADMIN"}},
		},
		{
			name:   "no source",
			tokens: []TokenConfig{{Role: "admin"}},
		},
		{
			name:   "both sources",
			tokens: []TokenConfig{{Role: "admin", TokenEnv: "FQ_TEST_ADMIN", TokenFile: "/tmp/x"}},
		},
		{
			name:   "secret too short",
			tokens: []TokenConfig{{Role: "admin", TokenEnv: "FQ_TEST_SHORT"}},
		},
		{
			name: "duplicate token",
			tokens: []TokenConfig{
				{Role: "admin", TokenEnv: "FQ_TEST_ADMIN"},
				{Role: "ro", TokenEnv: "FQ_TEST_DUP"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Network.Auth = AuthConfig{Tokens: test.tokens}

			err := resolveSecrets(&cfg)
			if err == nil {
				err = validate(&cfg)
			}

			require.Error(t, err)
		})
	}
}

func TestValidateRejectsReplicationWithoutSecret(t *testing.T) {
	cfg := validConfig()
	cfg.Replication.Auth = nil

	require.Error(t, validate(&cfg))
}

func TestValidateRejectsReplicationWithoutSecretSource(t *testing.T) {
	cfg := validConfig()
	cfg.Replication.Auth = &ReplicationAuthConfig{}

	require.Error(t, validate(&cfg))
}

func TestResolveSecretsRejectsUnsetReplicationEnv(t *testing.T) {
	cfg := validConfig()
	cfg.Replication.Auth = &ReplicationAuthConfig{TokenEnv: "FQ_TEST_ABSENT"}

	require.Error(t, resolveSecrets(&cfg))
}

func TestValidateRejectsHalfTLSKeyPair(t *testing.T) {
	cfg := validConfig()
	cfg.Network.TLS = TLSConfig{CertFile: "/tmp/server.crt"}

	require.Error(t, validate(&cfg))
}

func TestValidateRejectsUnknownTLSMinVersion(t *testing.T) {
	cfg := validConfig()
	cfg.Network.TLS = TLSConfig{
		CertFile:   "/tmp/server.crt",
		KeyFile:    "/tmp/server.key",
		MinVersion: "1.1",
	}

	require.Error(t, validate(&cfg))
}

func TestValidateRejectsReplicationMasterTLSWithoutServerCertificate(t *testing.T) {
	cfg := validConfig()
	cfg.Replication.ReplicaType = ReplicaTypeMaster
	cfg.Replication.ReplicaID = ""
	cfg.Replication.TLS = TLSConfig{ClientCAFile: "/tmp/clients.crt"}

	require.ErrorIs(t, validate(&cfg), security.ErrTLSCertRequired)
}

func TestValidateAllowsReplicationSlaveClientTLSWithoutServerCertificate(t *testing.T) {
	cfg := validConfig()
	cfg.Replication.ReplicaType = ReplicaTypeSlave
	cfg.Replication.TLS = TLSConfig{
		CAFile:     "/tmp/ca.crt",
		ServerName: "fq-master",
	}

	require.NoError(t, validate(&cfg))
}

func TestValidateAllowsEmptyNetworkAuth(t *testing.T) {
	cfg := validConfig()
	cfg.Network.Auth = AuthConfig{}

	require.NoError(t, validate(&cfg))
}

func TestResolveSecretsReadsReplicationTokenFromEnv(t *testing.T) {
	t.Setenv("FQ_TEST_REPLICATION", "replication-token-value")

	cfg := validConfig()
	cfg.Replication.Auth = &ReplicationAuthConfig{TokenEnv: "FQ_TEST_REPLICATION"}

	require.NoError(t, resolveSecrets(&cfg))
	require.Equal(t, "replication-token-value", cfg.Replication.Auth.ResolvedSecret().Reveal())
	require.NoError(t, validate(&cfg))
}

func TestTLSClientOptionsDropServerOnlyFields(t *testing.T) {
	cfg := TLSConfig{
		CertFile:     "/certs/server.crt",
		KeyFile:      "/certs/server.key",
		ClientCAFile: "/certs/ca.crt",
		ServerName:   "localhost",
	}

	options := cfg.ClientOptions()

	require.Empty(t, options.KeyFile)
	require.Empty(t, options.ClientCAFile)
	require.Equal(t, "localhost", options.ServerName)
	require.Equal(t, "/certs/server.crt", options.CAFile)
}

func TestTLSClientOptionsPreferExplicitCA(t *testing.T) {
	cfg := TLSConfig{CertFile: "/certs/server.crt", KeyFile: "/certs/server.key", CAFile: "/certs/ca.crt"}

	require.Equal(t, "/certs/ca.crt", cfg.ClientOptions().CAFile)
}

func TestTLSClientOptionsAreEmptyWithoutTLS(t *testing.T) {
	require.True(t, TLSConfig{}.ClientOptions().Empty())
}
