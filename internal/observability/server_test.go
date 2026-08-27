package observability

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestServerServesHealthAndMetrics(t *testing.T) {
	logger := zerolog.Nop()
	address := freeObservabilityAddress(t)
	server := NewServer(address, false, &logger)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Start(ctx)
	}()

	baseURL := "http://" + address
	client := &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	require.Eventually(t, func() bool {
		resp, err := client.Get(baseURL + "/healthz")
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()

		return resp.StatusCode == http.StatusOK
	}, time.Second, 10*time.Millisecond)

	resp, err := client.Get(baseURL + "/metrics")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Contains(t, string(body), "fq_tcp_active_connections")
	require.Contains(t, string(body), "# HELP")

	resp, err = client.Get(baseURL + "/debug/pprof/")
	require.NoError(t, err)
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.NoError(t, resp.Body.Close())

	cancel()
	require.NoError(t, <-done)
}

func TestServerServesPprofWhenEnabled(t *testing.T) {
	logger := zerolog.Nop()
	address := freeObservabilityAddress(t)
	server := NewServer(address, true, &logger)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- server.Start(ctx)
	}()

	baseURL := "http://" + address
	client := &http.Client{
		Transport: &http.Transport{DisableKeepAlives: true},
	}
	require.Eventually(t, func() bool {
		resp, err := client.Get(baseURL + "/debug/pprof/")
		if err != nil {
			return false
		}
		defer func() { _ = resp.Body.Close() }()

		return resp.StatusCode == http.StatusOK
	}, time.Second, 10*time.Millisecond)

	resp, err := client.Get(baseURL + "/debug/pprof/goroutine?debug=1")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Contains(t, string(body), "goroutine profile")

	cancel()
	require.NoError(t, <-done)
}

func freeObservabilityAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, listener.Close())
	}()

	return listener.Addr().String()
}
