package network

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"reflect"
	"sync/atomic"
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

func readFrame(conn net.Conn, maxMessageSize int) ([]byte, error) {
	frames := frameBuffer{}

	return frames.read(conn, maxMessageSize)
}

func writeFrame(conn net.Conn, payload []byte) error {
	var header [frameHeaderSize]byte

	return writeFrameWithHeader(conn, payload, header[:])
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

func TestTCPServerHandlesStreamedResponses(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	address := freeTCPAddress(t)
	maxMessageSize := 2048
	logger := zerolog.Nop()
	server, err := NewTCPServer(address, 10, maxMessageSize, time.Minute, &logger)
	require.NoError(t, err)

	go func() {
		require.NoError(t, server.HandleQueryStreams(ctx, func(
			_ context.Context,
			buffer []byte,
			write func([]byte) error,
		) error {
			require.Equal(t, []byte("stream"), buffer)
			require.NoError(t, write([]byte("first")))

			return write([]byte("second"))
		}))
	}()

	connection := dialEventually(t, address)
	defer func() { _ = connection.Close() }()

	require.NoError(t, writeFrame(connection, []byte("stream")))

	first, err := readFrame(connection, maxMessageSize)
	require.NoError(t, err)
	require.Equal(t, []byte("first"), first)

	second, err := readFrame(connection, maxMessageSize)
	require.NoError(t, err)
	require.Equal(t, []byte("second"), second)
}

func TestTCPServerCreatesRequestTimeoutOnlyForBlockingCommands(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	address := freeTCPAddress(t)
	maxMessageSize := 2048
	logger := zerolog.Nop()
	server, err := NewTCPServer(address, 10, maxMessageSize, time.Minute, &logger)
	require.NoError(t, err)

	deadlineByRequest := make(chan bool, 3)
	go func() {
		require.NoError(t, server.HandleQueries(ctx, func(ctx context.Context, buffer []byte) ([]byte, error) {
			_, hasDeadline := ctx.Deadline()
			deadlineByRequest <- hasDeadline

			return buffer, nil
		}))
	}()

	connection := dialEventually(t, address)
	defer func() { _ = connection.Close() }()

	for _, request := range []string{"INCR key 1", "WATCH key 1", " STREAM"} {
		require.NoError(t, writeFrame(connection, []byte(request)))

		response, err := readFrame(connection, maxMessageSize)
		require.NoError(t, err)
		require.Equal(t, []byte(request), response)
	}

	require.False(t, <-deadlineByRequest)
	require.True(t, <-deadlineByRequest)
	require.True(t, <-deadlineByRequest)
}

func TestRequestNeedsTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		request string
		want    bool
	}{
		{name: "empty", request: "", want: false},
		{name: "whitespace", request: " \t\n", want: false},
		{name: "incr", request: "INCR key 1", want: false},
		{name: "watch", request: "WATCH key 1", want: true},
		{name: "stream", request: "STREAM", want: true},
		{name: "pstream", request: "\tPSTREAM tenant-", want: true},
		{name: "lowercase stays nonblocking", request: "watch key 1", want: false},
		{name: "prefix is not command", request: "WATCHED key 1", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, requestNeedsTimeout([]byte(tt.request)))
		})
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

	var handled atomic.Bool
	go func() {
		require.NoError(t, server.HandleQueries(ctx, func(_ context.Context, buffer []byte) ([]byte, error) {
			handled.Store(true)
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
	require.False(t, handled.Load())
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

func TestReadFrameIntoRejectsSmallBuffer(t *testing.T) {
	t.Parallel()

	client, server := net.Pipe()
	defer func() { _ = server.Close() }()

	errCh := make(chan error, 1)
	go func() {
		header := make([]byte, frameHeaderSize)
		binary.BigEndian.PutUint32(header, 5)
		_, err := client.Write(header)
		errCh <- err
		_ = client.Close()
	}()

	_, err := readFrameInto(server, 2048, make([]byte, 4))
	require.ErrorIs(t, err, errFrameTooLarge)
	require.NoError(t, <-errCh)
}

func BenchmarkFrameBufferRoundTrip(b *testing.B) {
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	payload := []byte("INCR bench_key_123 600")
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		frames := frameBuffer{}
		for i := 0; i < b.N; i++ {
			message, err := frames.read(server, 2048)
			if err != nil {
				errCh <- err

				return
			}
			if err := frames.write(server, message); err != nil {
				errCh <- err

				return
			}
		}
	}()

	frames := frameBuffer{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := frames.write(client, payload); err != nil {
			b.Fatal(err)
		}
		message, err := frames.read(client, 2048)
		if err != nil {
			b.Fatal(err)
		}
		if !bytes.Equal(message, payload) {
			b.Fatalf("message = %q", message)
		}
	}
	b.StopTimer()

	if err := <-errCh; err != nil {
		b.Fatal(err)
	}
}

func BenchmarkTCPServerRequestContext(b *testing.B) {
	server := TCPServer{idleTimeout: time.Minute}
	ctx := context.Background()

	b.Run("incr", func(b *testing.B) {
		request := []byte("INCR bench_key_123 600")

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			requestCtx, cancel := server.requestContext(ctx, request)
			if requestCtx != ctx {
				b.Fatal("request context should be reused")
			}
			cancel()
		}
	})

	b.Run("watch", func(b *testing.B) {
		request := []byte("WATCH bench_key_123 600")

		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			requestCtx, cancel := server.requestContext(ctx, request)
			if requestCtx == ctx {
				b.Fatal("blocking request should use a timeout context")
			}
			cancel()
		}
	})
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
