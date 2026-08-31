package tui

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/dbcli"
	"github.com/fq-db/fq/internal/network"
)

func TestDialAddressRewritesWildcardHosts(t *testing.T) {
	cases := map[string]string{
		":1945":            "127.0.0.1:1945",
		"0.0.0.0:1945":     "127.0.0.1:1945",
		"localhost:1945":   "localhost:1945",
		"192.168.1.5:1945": "192.168.1.5:1945",
		"not-a-valid-addr": "not-a-valid-addr",
	}
	for input, want := range cases {
		if got := dialAddress(input); got != want {
			t.Errorf("dialAddress(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestDialWithRetrySucceedsOnceServerIsUp(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())

	logger := zerolog.Nop()
	server, err := network.NewTCPServer(address, 10, 4096, time.Minute, &logger)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() {
		time.Sleep(150 * time.Millisecond)
		_ = server.HandleQueryStreams(ctx, func(_ context.Context, _ []byte, write func([]byte) error) error {
			return write([]byte("ok|hi"))
		})
	}()

	client, err := dialWithRetry(
		context.Background(),
		dbcli.ConnectOptions{Address: address, MaxMessageSize: 4096, IdleTimeout: time.Minute},
		2*time.Second,
		20*time.Millisecond,
	)
	require.NoError(t, err)
	require.NotNil(t, client)
	_ = client.Close()
}

func TestDialWithRetryGivesUpAfterMaxWait(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())

	_, err = dialWithRetry(
		context.Background(),
		dbcli.ConnectOptions{Address: address, MaxMessageSize: 4096, IdleTimeout: time.Minute},
		100*time.Millisecond,
		10*time.Millisecond,
	)
	require.Error(t, err)
}
