package network

import (
	"context"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTCPClient(t *testing.T) {
	t.Parallel()

	request := "hello server"
	response := "hello client"

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}

		buffer, err := readFrame(connection, 2048)
		require.NoError(t, err)
		require.True(t, reflect.DeepEqual([]byte(request), buffer))

		require.NoError(t, writeFrame(connection, []byte(response)))

		defer func() {
			err = connection.Close()
			require.NoError(t, err)
			err = listener.Close()
			require.NoError(t, err)
		}()
	}()

	time.Sleep(100 * time.Millisecond)

	client, err := NewTCPClient(listener.Addr().String(), 2048, time.Minute)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	buffer, err := client.Send(context.Background(), []byte(request))
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual([]byte(response), buffer))
}

func TestTCPIdleClientConnection(t *testing.T) {
	t.Parallel()

	request := "hello server"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}

		buffer, err := readFrame(connection, 2048)
		require.NoError(t, err)
		require.True(t, reflect.DeepEqual([]byte(request), buffer))

		<-ctx.Done()
		defer func() {
			err = connection.Close()
			require.NoError(t, err)
			err = listener.Close()
			require.NoError(t, err)
		}()
	}()

	time.Sleep(100 * time.Millisecond)

	client, err := NewTCPClient(listener.Addr().String(), 2048, time.Millisecond*50)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	_, err = client.Send(context.Background(), []byte(request))
	require.Error(t, err)
}
