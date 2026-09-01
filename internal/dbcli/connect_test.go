package dbcli_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/dbcli"
)

func TestConnectSendsHelloWithToken(t *testing.T) {
	received := make(chan string, 1)
	address := startTestServer(t, func(_ context.Context, request []byte, write func([]byte) error) error {
		received <- string(request)

		return write([]byte("ok|1;8192;1;admin"))
	})

	client, err := dbcli.Connect(context.Background(), dbcli.ConnectOptions{
		Address:        address,
		MaxMessageSize: 8192,
		IdleTimeout:    time.Second,
		Token:          "admin-token-value",
	})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	require.Equal(t, "HELLO 1 AUTH admin-token-value", <-received)
}

func TestConnectSendsHelloWithoutToken(t *testing.T) {
	received := make(chan string, 1)
	address := startTestServer(t, func(_ context.Context, request []byte, write func([]byte) error) error {
		received <- string(request)

		return write([]byte("ok|1;8192;0;rw"))
	})

	client, err := dbcli.Connect(context.Background(), dbcli.ConnectOptions{
		Address:        address,
		MaxMessageSize: 8192,
		IdleTimeout:    time.Second,
	})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	require.Equal(t, "HELLO 1", <-received)
}

func TestConnectFailsOnRejectedAuth(t *testing.T) {
	address := startTestServer(t, func(_ context.Context, _ []byte, write func([]byte) error) error {
		return write([]byte("err|3002|authentication failed"))
	})

	_, err := dbcli.Connect(context.Background(), dbcli.ConnectOptions{
		Address:        address,
		MaxMessageSize: 8192,
		IdleTimeout:    time.Second,
		Token:          "bad-token-value",
	})
	require.Error(t, err)
}

func TestConnectFailsWhenServerClosesConnection(t *testing.T) {
	address := startTestServer(t, func(_ context.Context, _ []byte, _ func([]byte) error) error {
		return errors.New("closing")
	})

	_, err := dbcli.Connect(context.Background(), dbcli.ConnectOptions{
		Address:        address,
		MaxMessageSize: 8192,
		IdleTimeout:    time.Second,
		Token:          "any-token-value",
	})
	require.Error(t, err)
}
