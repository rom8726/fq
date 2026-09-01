package stress

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/fq-db/fq/internal/network"
)

func TestVerifierNegotiatesProtocolBeforeQuery(t *testing.T) {
	requests := make(chan string, 2)
	address := startVerifierTestServer(t, func(_ context.Context, request []byte, write func([]byte) error) error {
		requests <- string(request)
		if string(request) == "HELLO 1" {
			return write([]byte("ok|1;8192;0;admin"))
		}

		return write([]byte("ok|42"))
	})

	verifier := NewVerifier(address, 8192, time.Second)
	response, err := verifier.Query(context.Background(), "GET key 60")
	if err != nil {
		t.Fatal(err)
	}
	if response != "42" {
		t.Fatalf("response = %q, want %q", response, "42")
	}
	if got := <-requests; got != "HELLO 1" {
		t.Fatalf("first request = %q, want HELLO", got)
	}
	if got := <-requests; got != "GET key 60" {
		t.Fatalf("second request = %q, want query", got)
	}
}

func startVerifierTestServer(t *testing.T, handler network.TCPStreamHandler) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	logger := zerolog.Nop()
	server, err := network.NewTCPServer(address, 10, 8192, time.Minute, &logger)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() { _ = server.HandleQueryStreams(ctx, handler) }()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("tcp", address)
		if err == nil {
			_ = conn.Close()
			return address
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("server did not start at %s", address)
	return ""
}
