package network

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/security"
)

type testConnKey struct{}

func dialClientEventually(t *testing.T, address string, options ...ClientOption) *TCPClient {
	t.Helper()

	var (
		client *TCPClient
		err    error
	)

	for i := 0; i < 50; i++ {
		client, err = NewTCPClient(address, 4096, time.Second, options...)
		if err == nil {
			return client
		}

		time.Sleep(20 * time.Millisecond)
	}

	require.NoError(t, err)

	return nil
}

func TestServerAndClientOverTLS(t *testing.T) {
	pki := newTestPKI(t)
	logger := zerolog.Nop()

	serverTLS, err := security.TLSOptions{
		CertFile: pki.ServerCertFile,
		KeyFile:  pki.ServerKeyFile,
	}.ServerConfig()
	require.NoError(t, err)

	address := freeTCPAddress(t)

	server, err := NewTCPServer(address, 10, 4096, time.Minute, &logger, WithTLS(serverTLS))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = server.HandleQueries(ctx, func(_ context.Context, request []byte) ([]byte, error) {
			return append([]byte("ok|echo:"), request...), nil
		})
	}()

	clientTLS, err := security.TLSOptions{
		CAFile:     pki.CAFile,
		ServerName: "localhost",
	}.ClientConfig()
	require.NoError(t, err)

	client := dialClientEventually(t, address, WithClientTLS(clientTLS))
	defer func() { _ = client.Close() }()

	response, err := client.Send(context.Background(), []byte("hello"))
	require.NoError(t, err)
	require.Equal(t, "echo:hello", string(response))
}

func TestPlaintextClientCannotTalkToTLSServer(t *testing.T) {
	pki := newTestPKI(t)
	logger := zerolog.Nop()

	serverTLS, err := security.TLSOptions{
		CertFile: pki.ServerCertFile,
		KeyFile:  pki.ServerKeyFile,
	}.ServerConfig()
	require.NoError(t, err)

	address := freeTCPAddress(t)

	server, err := NewTCPServer(address, 10, 4096, time.Second, &logger, WithTLS(serverTLS))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = server.HandleQueries(ctx, func(_ context.Context, request []byte) ([]byte, error) {
			return append([]byte("ok|"), request...), nil
		})
	}()

	client := dialClientEventually(t, address)
	defer func() { _ = client.Close() }()

	response, err := client.Send(context.Background(), []byte("hello"))
	if err == nil {
		require.Empty(t, response)
	}
}

func TestServerRequiresClientCertificateForMutualTLS(t *testing.T) {
	pki := newTestPKI(t)
	logger := zerolog.Nop()

	serverTLS, err := security.TLSOptions{
		CertFile:     pki.ServerCertFile,
		KeyFile:      pki.ServerKeyFile,
		ClientCAFile: pki.CAFile,
	}.ServerConfig()
	require.NoError(t, err)

	address := freeTCPAddress(t)

	server, err := NewTCPServer(address, 10, 4096, time.Second, &logger, WithTLS(serverTLS))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = server.HandleQueries(ctx, func(_ context.Context, request []byte) ([]byte, error) {
			return append([]byte("ok|"), request...), nil
		})
	}()

	withoutCert, err := security.TLSOptions{CAFile: pki.CAFile, ServerName: "localhost"}.ClientConfig()
	require.NoError(t, err)

	anonymous := dialClientEventually(t, address, WithClientTLS(withoutCert))
	defer func() { _ = anonymous.Close() }()

	rejected, err := anonymous.Send(context.Background(), []byte("hello"))
	if err == nil {
		require.Empty(t, rejected)
	}

	withCert, err := security.TLSOptions{
		CAFile:     pki.CAFile,
		ServerName: "localhost",
		CertFile:   pki.ClientCertFile,
		KeyFile:    pki.ClientKeyFile,
	}.ClientConfig()
	require.NoError(t, err)

	authorized := dialClientEventually(t, address, WithClientTLS(withCert))
	defer func() { _ = authorized.Close() }()

	response, err := authorized.Send(context.Background(), []byte("hello"))
	require.NoError(t, err)
	require.Equal(t, "hello", string(response))
}

func TestConnContextRunsOncePerConnection(t *testing.T) {
	logger := zerolog.Nop()
	address := freeTCPAddress(t)

	var calls atomic.Int64

	server, err := NewTCPServer(address, 10, 4096, time.Minute, &logger,
		WithConnContext(func(ctx context.Context, conn net.Conn) context.Context {
			calls.Add(1)

			return context.WithValue(ctx, testConnKey{}, conn.RemoteAddr().String())
		}),
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = server.HandleQueries(ctx, func(ctx context.Context, _ []byte) ([]byte, error) {
			value, _ := ctx.Value(testConnKey{}).(string)

			return append([]byte("ok|"), value...), nil
		})
	}()

	client := dialClientEventually(t, address)
	defer func() { _ = client.Close() }()

	first, err := client.Send(context.Background(), []byte("a"))
	require.NoError(t, err)
	require.NotEmpty(t, string(first))

	second, err := client.Send(context.Background(), []byte("b"))
	require.NoError(t, err)
	require.Equal(t, string(first), string(second))
	require.Equal(t, int64(1), calls.Load())
}
