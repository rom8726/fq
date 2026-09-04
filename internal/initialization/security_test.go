package initialization

import (
	"bytes"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/config"
	"github.com/fq-db/fq/internal/database/storage/replication"
	"github.com/fq-db/fq/internal/security"
)

func TestCreateReplicaRejectsMasterWithoutSecret(t *testing.T) {
	logger := zerolog.Nop()

	_, err := CreateReplica(
		config.ReplicationConfig{
			ReplicaType:   config.ReplicaTypeMaster,
			MasterAddress: ":19460",
			SyncInterval:  time.Second,
		},
		nil, replication.Compression{}, &logger, nil, nil, nil,
	)

	require.Error(t, err)
}

func TestCreateReplicaRejectsSlaveWithoutSecret(t *testing.T) {
	logger := zerolog.Nop()

	_, err := CreateReplica(
		config.ReplicationConfig{
			ReplicaType:   config.ReplicaTypeSlave,
			ReplicaID:     "replica-1",
			MasterAddress: ":19460",
			SyncInterval:  time.Second,
		},
		nil, replication.Compression{}, &logger, nil, nil, nil,
	)

	require.Error(t, err)
}

func TestCreateReplicaSkipsWhenReplicationIsDisabled(t *testing.T) {
	logger := zerolog.Nop()

	replica, err := CreateReplica(config.ReplicationConfig{}, nil, replication.Compression{}, &logger, nil, nil, nil)

	require.NoError(t, err)
	require.Nil(t, replica)
}

func TestBuildRegistryIsDisabledForEmptyConfig(t *testing.T) {
	registry, err := BuildRegistry(config.AuthConfig{})

	require.NoError(t, err)
	require.False(t, registry.Enabled())
}

func TestCreateNetworkInstallsSessionPerConnection(t *testing.T) {
	logger := zerolog.Nop()
	registry := security.NewRegistry()
	require.NoError(t, registry.Add("admin-token-value", security.RoleAdmin))

	server, err := CreateNetwork(config.NetworkConfig{
		Address:        "127.0.0.1:0",
		MaxConnections: 10,
		MaxMessageSize: "4KB",
		IdleTimeout:    time.Second,
	}, registry, &logger)

	require.NoError(t, err)
	require.NotNil(t, server)
}

func TestCreateNetworkRejectsBrokenTLS(t *testing.T) {
	logger := zerolog.Nop()

	_, err := CreateNetwork(config.NetworkConfig{
		Address:        "127.0.0.1:0",
		MaxConnections: 10,
		MaxMessageSize: "4KB",
		IdleTimeout:    time.Second,
		TLS: config.TLSConfig{
			CertFile: "/nonexistent/server.crt",
			KeyFile:  "/nonexistent/server.key",
		},
	}, security.NewRegistry(), &logger)

	require.Error(t, err)
}

func TestReadOnlyIsTrueOnlyForSlave(t *testing.T) {
	require.False(t, (&Initializer{}).readOnly())
	require.False(t, (&Initializer{master: &replication.Master{}}).readOnly())
	require.True(t, (&Initializer{slave: &replication.Slave{}}).readOnly())
}

func TestWarnCleartextAuthLogsWhenTLSIsDisabled(t *testing.T) {
	var buffer bytes.Buffer
	logger := zerolog.New(&buffer)

	warnCleartextAuth(&logger, clientPortName, "127.0.0.1:1945", true, false)

	require.Contains(t, buffer.String(), "cleartext")
	require.Contains(t, buffer.String(), clientPortName)
	require.Contains(t, buffer.String(), "127.0.0.1:1945")
}

func TestWarnCleartextAuthIsSilentWhenTLSIsEnabled(t *testing.T) {
	var buffer bytes.Buffer
	logger := zerolog.New(&buffer)

	warnCleartextAuth(&logger, replicationPortName, ":1946", true, true)

	require.Empty(t, buffer.String())
}

func TestWarnCleartextAuthIsSilentWithoutAuth(t *testing.T) {
	var buffer bytes.Buffer
	logger := zerolog.New(&buffer)

	warnCleartextAuth(&logger, clientPortName, ":1945", false, false)

	require.Empty(t, buffer.String())
}

func TestCreateNetworkWarnsAboutCleartextTokens(t *testing.T) {
	var buffer bytes.Buffer
	logger := zerolog.New(&buffer)

	registry := security.NewRegistry()
	require.NoError(t, registry.Add("admin-token-value", security.RoleAdmin))

	_, err := CreateNetwork(config.NetworkConfig{
		Address:        "127.0.0.1:0",
		MaxConnections: 10,
		MaxMessageSize: "4KB",
		IdleTimeout:    time.Second,
	}, registry, &logger)

	require.NoError(t, err)
	require.Contains(t, buffer.String(), "cleartext")
}
