package integration

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/config"
	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/database/compute"
	"github.com/fq-db/fq/internal/database/storage"
	inmemory "github.com/fq-db/fq/internal/database/storage/engine/in-memory"
	"github.com/fq-db/fq/internal/database/storage/replication"
	"github.com/fq-db/fq/internal/database/storage/wal"
	"github.com/fq-db/fq/internal/dbcli"
	"github.com/fq-db/fq/internal/network"
	"github.com/fq-db/fq/internal/security"
)

func startSecuredTestServer(t *testing.T, registry *security.Registry) string {
	t.Helper()

	logger := zerolog.Nop()
	walStream := make(chan wal.Chunk, 8)
	dumpStream := make(chan database.DumpChunk, 1)

	engine, err := inmemory.NewEngine(inmemory.HashTableBuilder, 4, &logger, walStream, dumpStream)
	require.NoError(t, err)

	strg, err := storage.NewStorage(
		engine, nil, nil, nil, &logger,
		time.Hour, time.Hour, true, config.DefaultLimitEventQueueCapacity,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	strg.Start(ctx)

	comp := compute.NewCompute(compute.NewParser(&logger), compute.NewAnalyzer(&logger), &logger)
	db := database.NewDatabase(comp, strg, &logger, 64<<10)

	address := freeLocalAddress(t)
	server, err := network.NewTCPServer(address, 32, 64<<10, time.Second, &logger,
		network.WithConnContext(func(ctx context.Context, _ net.Conn) context.Context {
			return security.WithSession(ctx, security.NewSession(registry))
		}),
	)
	require.NoError(t, err)

	go func() {
		_ = server.HandleQueryStreams(ctx, func(
			ctx context.Context,
			query []byte,
			write func([]byte) error,
		) error {
			return db.HandleQueryStream(ctx, string(query), write)
		})
	}()

	require.Eventually(t, func() bool {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			return false
		}
		_ = conn.Close()

		return true
	}, 2*time.Second, 10*time.Millisecond)

	return address
}

func connectWithToken(t *testing.T, address, token string) *network.TCPClient {
	t.Helper()

	client, err := dbcli.Connect(context.Background(), dbcli.ConnectOptions{
		Address:        address,
		MaxMessageSize: 64 << 10,
		IdleTimeout:    2 * time.Second,
		Token:          token,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func securedRegistry(t *testing.T) *security.Registry {
	t.Helper()

	registry := security.NewRegistry()
	require.NoError(t, registry.Add("admin-token-value", security.RoleAdmin))
	require.NoError(t, registry.Add("rw-token-value", security.RoleRW))
	require.NoError(t, registry.Add("ro-token-value", security.RoleRO))

	return registry
}

func TestClientPortRejectsUnauthenticatedDestructiveCommands(t *testing.T) {
	address := startSecuredTestServer(t, securedRegistry(t))
	client := connectWithToken(t, address, "")

	for _, query := range []string{"FLUSHDB", "TRUNCATE", "GET key 60", "INCR key 60"} {
		response, err := client.Send(context.Background(), []byte(query))
		require.NoError(t, err, query)
		require.Contains(t, string(response), "not authenticated", query)
	}
}

func TestClientPortEnforcesRoles(t *testing.T) {
	address := startSecuredTestServer(t, securedRegistry(t))

	readOnly := connectWithToken(t, address, "ro-token-value")
	response, err := readOnly.Send(context.Background(), []byte("TRUNCATE"))
	require.NoError(t, err)
	require.Contains(t, string(response), "permission denied")

	response, err = readOnly.Send(context.Background(), []byte("GET key 60"))
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(response), "ok|"), string(response))

	readWrite := connectWithToken(t, address, "rw-token-value")
	response, err = readWrite.Send(context.Background(), []byte("INCR key 60"))
	require.NoError(t, err)
	require.Equal(t, "ok|1", string(response))

	response, err = readWrite.Send(context.Background(), []byte("FLUSHDB"))
	require.NoError(t, err)
	require.Contains(t, string(response), "permission denied")

	admin := connectWithToken(t, address, "admin-token-value")
	response, err = admin.Send(context.Background(), []byte("FLUSHDB"))
	require.NoError(t, err)
	require.Equal(t, "ok|1", string(response))
}

func TestClientPortSessionsAreIndependent(t *testing.T) {
	address := startSecuredTestServer(t, securedRegistry(t))

	admin := connectWithToken(t, address, "admin-token-value")
	response, err := admin.Send(context.Background(), []byte("TRUNCATE"))
	require.NoError(t, err)
	require.Equal(t, "ok|1", string(response))

	anonymous := connectWithToken(t, address, "")
	response, err = anonymous.Send(context.Background(), []byte("TRUNCATE"))
	require.NoError(t, err)
	require.Contains(t, string(response), "not authenticated")
}

func TestClientPortRejectsBadToken(t *testing.T) {
	address := startSecuredTestServer(t, securedRegistry(t))

	_, err := dbcli.Connect(context.Background(), dbcli.ConnectOptions{
		Address:        address,
		MaxMessageSize: 64 << 10,
		IdleTimeout:    2 * time.Second,
		Token:          "wrong-token-value",
	})

	require.ErrorIs(t, err, dbcli.ErrAuthenticationRejected)
}

func TestReplicationPortRejectsUnauthenticatedPeer(t *testing.T) {
	logger := zerolog.Nop()
	address := freeLocalAddress(t)

	server, err := network.NewTCPServer(address, 5, 16<<20, time.Second, &logger)
	require.NoError(t, err)

	master, err := replication.NewMaster(
		server, t.TempDir(), nil, security.Secret(testReplicationToken), &logger,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() { _ = master.Start(ctx) }()

	require.Eventually(t, func() bool {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			return false
		}
		_ = conn.Close()

		return true
	}, 2*time.Second, 10*time.Millisecond)

	factory := replication.NewTCPClientFactory(address, 16<<20, time.Second, nil)

	impostor, err := factory.Create()
	require.NoError(t, err)
	defer func() { _ = impostor.Close() }()

	request := replication.NewWALRequest("wrong-token-value", "replica-1", "", 0, 0)
	data, err := replication.Encode(&request)
	require.NoError(t, err)

	response, err := impostor.Send(context.Background(), data)
	if err == nil {
		require.Empty(t, response)
	}

	legitimate, err := factory.Create()
	require.NoError(t, err)
	defer func() { _ = legitimate.Close() }()

	request = replication.NewWALRequest(testReplicationToken, "replica-1", "", 0, 0)
	data, err = replication.Encode(&request)
	require.NoError(t, err)

	response, err = legitimate.Send(context.Background(), data)
	require.NoError(t, err)
	require.NotEmpty(t, response)
}

func TestReplicationPortRefusesDumpToImpostor(t *testing.T) {
	logger := zerolog.Nop()
	address := freeLocalAddress(t)

	server, err := network.NewTCPServer(address, 5, 16<<20, time.Second, &logger)
	require.NoError(t, err)

	master, err := replication.NewMaster(
		server, t.TempDir(), nil, security.Secret(testReplicationToken), &logger,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() { _ = master.Start(ctx) }()

	require.Eventually(t, func() bool {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			return false
		}
		_ = conn.Close()

		return true
	}, 2*time.Second, 10*time.Millisecond)

	factory := replication.NewTCPClientFactory(address, 16<<20, time.Second, nil)
	impostor, err := factory.Create()
	require.NoError(t, err)
	defer func() { _ = impostor.Close() }()

	request := replication.NewDumpRequest("", "session-uuid", 0)
	data, err := replication.Encode(&request)
	require.NoError(t, err)

	response, err := impostor.Send(context.Background(), data)
	if err == nil {
		require.Empty(t, response)
	}
}

func TestReplicationPortOverMutualTLS(t *testing.T) {
	pki := newTestPKI(t)
	logger := zerolog.Nop()
	address := freeLocalAddress(t)

	serverTLS, err := security.TLSOptions{
		CertFile:     pki.ServerCertFile,
		KeyFile:      pki.ServerKeyFile,
		ClientCAFile: pki.CAFile,
	}.ServerConfig()
	require.NoError(t, err)

	server, err := network.NewTCPServer(address, 5, 16<<20, time.Second, &logger, network.WithTLS(serverTLS))
	require.NoError(t, err)

	master, err := replication.NewMaster(
		server, t.TempDir(), nil, security.Secret(testReplicationToken), &logger,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() { _ = master.Start(ctx) }()

	require.Eventually(t, func() bool {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			return false
		}
		_ = conn.Close()

		return true
	}, 2*time.Second, 10*time.Millisecond)

	replicaTLS, err := security.TLSOptions{
		CAFile:     pki.CAFile,
		ServerName: "localhost",
		CertFile:   pki.ClientCertFile,
		KeyFile:    pki.ClientKeyFile,
	}.ClientConfig()
	require.NoError(t, err)

	replica, err := replication.NewTCPClientFactory(address, 16<<20, time.Second, replicaTLS).Create()
	require.NoError(t, err)
	defer func() { _ = replica.Close() }()

	request := replication.NewWALRequest(testReplicationToken, "replica-1", "", 0, 0)
	data, err := replication.Encode(&request)
	require.NoError(t, err)

	response, err := replica.Send(context.Background(), data)
	require.NoError(t, err)
	require.NotEmpty(t, response)

	outsiderTLS, err := security.TLSOptions{CAFile: pki.CAFile, ServerName: "localhost"}.ClientConfig()
	require.NoError(t, err)

	outsider, err := replication.NewTCPClientFactory(address, 16<<20, time.Second, outsiderTLS).Create()
	require.NoError(t, err)
	defer func() { _ = outsider.Close() }()

	rejected, err := outsider.Send(context.Background(), data)
	if err == nil {
		require.Empty(t, rejected)
	}
}
