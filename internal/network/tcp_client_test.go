package network

import (
	"context"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/protocol"
)

func TestTCPClient(t *testing.T) {
	t.Parallel()

	request := "hello server"
	response := "ok|hello client"
	expectedBody := "hello client"

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
	require.True(t, reflect.DeepEqual([]byte(expectedBody), buffer))
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
	require.ErrorIs(t, err, ErrIdleTimeout)
}

func newTestClient(t *testing.T, respond func(request []byte) []byte) *TCPClient {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer func() { _ = connection.Close() }()

		for {
			request, readErr := readFrame(connection, 65536)
			if readErr != nil {
				return
			}

			if writeErr := writeFrame(connection, respond(request)); writeErr != nil {
				return
			}
		}
	}()

	client, err := NewTCPClient(listener.Addr().String(), 2048, time.Minute)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func TestSendReturnsBodyWithoutPrefix(t *testing.T) {
	client := newTestClient(t, func(request []byte) []byte {
		return []byte("ok|42")
	})

	response, err := client.Send(context.Background(), []byte("GET key 60"))
	require.NoError(t, err)
	require.Equal(t, "42", string(response))
}

func TestSendReturnsTypedProtocolError(t *testing.T) {
	client := newTestClient(t, func(request []byte) []byte {
		return []byte("err|4000|quota not found")
	})

	_, err := client.Send(context.Background(), []byte("QUOTA INF q"))

	var protoErr *protocol.Error
	require.ErrorAs(t, err, &protoErr)
	require.Equal(t, protocol.CodeQuotaNotFound, protoErr.Code)
}

func TestHelloParsesServerInfoAndAppliesMessageSize(t *testing.T) {
	client := newTestClient(t, func(request []byte) []byte {
		require.Equal(t, "HELLO 1", string(request))

		return []byte("ok|1;65536;1;admin")
	})

	info, err := client.Hello(context.Background(), "")
	require.NoError(t, err)
	require.Equal(t, uint16(1), info.Version)
	require.Equal(t, 65536, info.MaxMessageSize)
	require.True(t, info.AuthRequired)
	require.Equal(t, "admin", info.Role)
	require.Equal(t, 65536, client.maxMessageSize)
}

func TestSendRawReturnsFrameBytesUnparsed(t *testing.T) {
	rawResponse := []byte{0x7f, 0x03, 0x01, 0x02, 0xff, 0xfe, 0x00, 'd', 'a', 't', 'a'}

	client := newTestClient(t, func(request []byte) []byte {
		return rawResponse
	})

	response, err := client.SendRaw(context.Background(), []byte("wal request"))
	require.NoError(t, err)
	require.Equal(t, rawResponse, response)
}

func TestHelloSendsToken(t *testing.T) {
	client := newTestClient(t, func(request []byte) []byte {
		require.Equal(t, "HELLO 1 AUTH s3cret", string(request))

		return []byte("ok|1;4096;1;rw")
	})

	info, err := client.Hello(context.Background(), "s3cret")
	require.NoError(t, err)
	require.Equal(t, "rw", info.Role)
}
