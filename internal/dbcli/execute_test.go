package dbcli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/dbcli"
	"github.com/fq-db/fq/internal/inspect"
	"github.com/fq-db/fq/internal/network"
	"github.com/fq-db/fq/internal/protocol"
)

func startTestServer(t *testing.T, handler network.TCPStreamHandler) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())

	logger := zerolog.Nop()
	server, err := network.NewTCPServer(address, 10, 8192, time.Minute, &logger)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() {
		_ = server.HandleQueryStreams(ctx, handler)
	}()

	require.Eventually(t, func() bool {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, time.Second, 10*time.Millisecond)

	return address
}

func dialTestClient(t *testing.T, address string, idleTimeout time.Duration) *network.TCPClient {
	t.Helper()

	client, err := network.NewTCPClient(address, 8192, idleTimeout)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func TestExecutePlainOK(t *testing.T) {
	t.Parallel()

	address := startTestServer(t, func(_ context.Context, _ []byte, write func([]byte) error) error {
		return write([]byte("ok|plain-result"))
	})
	client := dialTestClient(t, address, time.Minute)

	var out bytes.Buffer
	logger := zerolog.Nop()
	err := dbcli.Execute(context.Background(), &logger, client, "GET foo", &out, time.Now())

	require.NoError(t, err)
	require.Contains(t, out.String(), "plain-result")
	require.Contains(t, out.String(), "Elapsed:")
}

func TestExecutePlainError(t *testing.T) {
	t.Parallel()

	address := startTestServer(t, func(_ context.Context, _ []byte, write func([]byte) error) error {
		return write([]byte("err|9000|boom"))
	})
	client := dialTestClient(t, address, time.Minute)

	var out bytes.Buffer
	logger := zerolog.Nop()
	err := dbcli.Execute(context.Background(), &logger, client, "GET missing", &out, time.Now())

	require.NoError(t, err)
	require.Contains(t, out.String(), "[9000] boom")
}

func TestExecutePlainMalformedResponse(t *testing.T) {
	t.Parallel()

	address := startTestServer(t, func(_ context.Context, _ []byte, write func([]byte) error) error {
		return write([]byte("plain-result"))
	})
	client := dialTestClient(t, address, time.Minute)

	var out bytes.Buffer
	logger := zerolog.Nop()
	err := dbcli.Execute(context.Background(), &logger, client, "GET malformed", &out, time.Now())

	require.Error(t, err)
	require.ErrorIs(t, err, protocol.ErrMalformedResponse)
}

func TestExecutePrintsErrorWithCode(t *testing.T) {
	t.Parallel()

	address := startTestServer(t, func(_ context.Context, _ []byte, write func([]byte) error) error {
		return write([]byte("err|4000|quota not found"))
	})
	client := dialTestClient(t, address, time.Minute)

	var out bytes.Buffer
	logger := zerolog.Nop()
	require.NoError(t, dbcli.Execute(context.Background(), &logger, client, "QUOTA INF q", &out, time.Now()))
	require.Contains(t, out.String(), "[4000] quota not found")
}

func TestExecuteInspect(t *testing.T) {
	t.Parallel()

	report := inspect.Report{Section: "wal", TS: 1700000000}
	body, err := json.Marshal(report)
	require.NoError(t, err)

	address := startTestServer(t, func(_ context.Context, query []byte, write func([]byte) error) error {
		require.Equal(t, "INSPECT", string(query))
		return write(append([]byte("ok|"), body...))
	})
	client := dialTestClient(t, address, time.Minute)

	var out bytes.Buffer
	logger := zerolog.Nop()
	err = dbcli.Execute(context.Background(), &logger, client, "INSPECT", &out, time.Now())

	require.NoError(t, err)
	require.Contains(t, out.String(), `"section": "wal"`)
}

func TestExecuteHumanInspect(t *testing.T) {
	t.Parallel()

	report := inspect.Report{
		Section:  "wal",
		TS:       1700000000,
		Instance: &inspect.InstanceInfo{Role: "master", Version: "1.0.0", ListenAddr: ":1945"},
	}
	body, err := json.Marshal(report)
	require.NoError(t, err)

	address := startTestServer(t, func(_ context.Context, query []byte, write func([]byte) error) error {
		require.Equal(t, "INSPECT wal", string(query))
		return write(append([]byte("ok|"), body...))
	})
	client := dialTestClient(t, address, time.Minute)

	var out bytes.Buffer
	logger := zerolog.Nop()
	err = dbcli.Execute(context.Background(), &logger, client, "HINSPECT wal", &out, time.Now())

	require.NoError(t, err)
	require.Contains(t, out.String(), "INSTANCE")
	require.Contains(t, out.String(), "1.0.0")
}

func TestExecuteStreamIdleTimeout(t *testing.T) {
	t.Parallel()

	address := startTestServer(t, func(ctx context.Context, _ []byte, write func([]byte) error) error {
		if err := write([]byte("ok|first-frame")); err != nil {
			return err
		}
		<-ctx.Done()
		return ctx.Err()
	})
	client := dialTestClient(t, address, 20*time.Millisecond)

	var out bytes.Buffer
	logger := zerolog.Nop()
	err := dbcli.Execute(context.Background(), &logger, client, "STREAM", &out, time.Now())

	require.NoError(t, err)
	require.Contains(t, out.String(), "Streaming events")
	require.Contains(t, out.String(), "first-frame")
	require.Contains(t, out.String(), "Stream idle timeout")
}

func TestExecuteWatchAdvisoryMessage(t *testing.T) {
	t.Parallel()

	address := startTestServer(t, func(ctx context.Context, _ []byte, _ func([]byte) error) error {
		<-ctx.Done()
		return ctx.Err()
	})
	client := dialTestClient(t, address, 20*time.Millisecond)

	var out bytes.Buffer
	logger := zerolog.Nop()
	err := dbcli.Execute(context.Background(), &logger, client, "WATCH foo", &out, time.Now())

	require.Error(t, err)
	require.Contains(t, out.String(), "Watching for changes")
}
