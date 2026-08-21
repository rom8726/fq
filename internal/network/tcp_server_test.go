package network

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestTCPServer(t *testing.T) {
	t.Parallel()

	request := "hello server"
	response := "hello client"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	address := freeTCPAddress(t)
	maxMessageSize := 2048
	maxConnectionsNumber := 10
	idleTimeout := time.Minute
	logger := zerolog.Nop()
	server, err := NewTCPServer(address, maxConnectionsNumber, maxMessageSize, idleTimeout, &logger)
	require.NoError(t, err)

	go func() {
		require.NoError(t, server.HandleQueries(ctx, func(ctx context.Context, buffer []byte) ([]byte, error) {
			require.True(t, reflect.DeepEqual([]byte(request), buffer))
			return []byte(response), nil
		}))
	}()

	connection := dialEventually(t, address)
	defer func() { _ = connection.Close() }()

	err = writeFrame(connection, []byte(request))
	require.NoError(t, err)

	buffer, err := readFrame(connection, maxMessageSize)
	require.NoError(t, err)
	require.True(t, reflect.DeepEqual([]byte(response), buffer))
}

func TestTCPServerHandlesMultipleFrames(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	address := freeTCPAddress(t)
	maxMessageSize := 2048
	logger := zerolog.Nop()
	server, err := NewTCPServer(address, 10, maxMessageSize, time.Minute, &logger)
	require.NoError(t, err)

	go func() {
		require.NoError(t, server.HandleQueries(ctx, func(_ context.Context, buffer []byte) ([]byte, error) {
			return append([]byte("echo:"), buffer...), nil
		}))
	}()

	connection := dialEventually(t, address)
	defer func() { _ = connection.Close() }()

	for _, request := range [][]byte{[]byte("first"), []byte("second")} {
		require.NoError(t, writeFrame(connection, request))

		response, err := readFrame(connection, maxMessageSize)
		require.NoError(t, err)
		require.Equal(t, append([]byte("echo:"), request...), response)
	}
}

func TestTCPServerRejectsOversizedFrame(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	address := freeTCPAddress(t)
	maxMessageSize := 4
	logger := zerolog.Nop()
	server, err := NewTCPServer(address, 10, maxMessageSize, time.Minute, &logger)
	require.NoError(t, err)

	go func() {
		require.NoError(t, server.HandleQueries(ctx, func(_ context.Context, buffer []byte) ([]byte, error) {
			return buffer, nil
		}))
	}()

	connection := dialEventually(t, address)
	defer func() { _ = connection.Close() }()

	header := make([]byte, frameHeaderSize)
	binary.BigEndian.PutUint32(header, uint32(maxMessageSize+1))
	require.NoError(t, writeAll(connection, header))

	_, err = readFrame(connection, maxMessageSize)
	require.Error(t, err)
}

func TestReadFrameReturnsHeaderReadError(t *testing.T) {
	t.Parallel()

	client, server := net.Pipe()
	defer func() { _ = server.Close() }()

	errCh := make(chan error, 1)
	go func() {
		_, err := client.Write([]byte{0x00, 0x00})
		if err != nil {
			errCh <- err

			return
		}

		errCh <- client.Close()
	}()

	_, err := readFrame(server, 2048)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.NoError(t, <-errCh)
}

func TestReadFrameReturnsPayloadReadError(t *testing.T) {
	t.Parallel()

	client, server := net.Pipe()
	defer func() { _ = server.Close() }()

	errCh := make(chan error, 1)
	go func() {
		header := make([]byte, frameHeaderSize)
		binary.BigEndian.PutUint32(header, 4)
		if err := writeAll(client, header); err != nil {
			errCh <- err

			return
		}

		if _, err := client.Write([]byte("he")); err != nil {
			errCh <- err

			return
		}

		errCh <- client.Close()
	}()

	_, err := readFrame(server, 2048)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.NoError(t, <-errCh)
}

func freeTCPAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = listener.Close() }()

	return listener.Addr().String()
}

func dialEventually(t *testing.T, address string) net.Conn {
	t.Helper()

	var connection net.Conn
	require.Eventually(t, func() bool {
		var err error
		connection, err = net.Dial("tcp", address)

		return err == nil
	}, time.Second, 10*time.Millisecond)

	return connection
}
