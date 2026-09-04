package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestIsNetworkErrorDetectsBrokenConnection(t *testing.T) {
	slave := &Slave{}

	for _, tc := range []struct {
		name string
		err  error
	}{
		{"writev broken pipe", &net.OpError{Op: "writev", Net: "tcp", Err: syscall.EPIPE}},
		{"write broken pipe", &net.OpError{Op: "write", Net: "tcp", Err: syscall.EPIPE}},
		{"read connection reset", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}},
		{"dial connection refused", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}},
		{"wrapped writev broken pipe", fmt.Errorf(
			"send request: %w", &net.OpError{Op: "writev", Net: "tcp", Err: syscall.EPIPE})},
		{"use of closed connection", net.ErrClosed},
		{"eof", io.EOF},
		{"unexpected eof", io.ErrUnexpectedEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, slave.isNetworkError(tc.err))
		})
	}
}

func TestIsNetworkErrorIgnoresApplicationErrors(t *testing.T) {
	slave := &Slave{}

	require.False(t, slave.isNetworkError(nil))
	require.False(t, slave.isNetworkError(errors.New("dump batch codec mismatch")))
}

func newRetryTestSlave() *Slave {
	logger := zerolog.Nop()

	return &Slave{
		closeCh:       make(chan struct{}),
		maxRetries:    1,
		retryDelay:    time.Millisecond,
		maxRetryDelay: time.Hour,
		logger:        &logger,
	}
}

func TestHandleSyncErrorWaitModeIsInterruptedByShutdown(t *testing.T) {
	slave := newRetryTestSlave()

	done := make(chan struct{})
	go func() {
		defer close(done)
		slave.handleSyncError(context.Background(), errors.New("sync failed"), "wal")
	}()

	close(slave.closeCh)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("wait mode ignored shutdown")
	}
	require.Equal(t, 1, slave.consecutiveErrors)
}

func TestHandleSyncErrorWaitModeIsInterruptedByContext(t *testing.T) {
	slave := newRetryTestSlave()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		slave.handleSyncError(ctx, errors.New("sync failed"), "dump")
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("wait mode ignored context cancellation")
	}
	require.Equal(t, 1, slave.consecutiveErrors)
}

func TestHandleSyncErrorResetsCounterAfterCompletedWait(t *testing.T) {
	slave := newRetryTestSlave()
	slave.maxRetryDelay = time.Millisecond

	slave.handleSyncError(context.Background(), errors.New("sync failed"), "wal")

	require.Zero(t, slave.consecutiveErrors)
}

func TestHandleSyncErrorBelowMaxRetriesDoesNotWait(t *testing.T) {
	slave := newRetryTestSlave()
	slave.maxRetries = 3

	started := time.Now()
	slave.handleSyncError(context.Background(), errors.New("sync failed"), "wal")

	require.Equal(t, 1, slave.consecutiveErrors)
	require.Less(t, time.Since(started), time.Second)
}

func TestWaitBeforeRetryDoesNotDelayWithoutErrors(t *testing.T) {
	slave := newRetryTestSlave()
	slave.retryDelay = time.Hour

	started := time.Now()
	require.True(t, slave.waitBeforeRetry(context.Background()))
	require.Less(t, time.Since(started), time.Second)
}

func TestWaitBeforeRetryBacksOffAfterErrors(t *testing.T) {
	slave := newRetryTestSlave()
	slave.retryDelay = 50 * time.Millisecond
	slave.consecutiveErrors = 2

	started := time.Now()
	require.True(t, slave.waitBeforeRetry(context.Background()))
	require.GreaterOrEqual(t, time.Since(started), 100*time.Millisecond)
}

func TestWaitBeforeRetryStopsOnShutdown(t *testing.T) {
	slave := newRetryTestSlave()
	slave.retryDelay = time.Hour
	slave.consecutiveErrors = 1

	result := make(chan bool, 1)
	go func() { result <- slave.waitBeforeRetry(context.Background()) }()

	close(slave.closeCh)

	select {
	case ok := <-result:
		require.False(t, ok)
	case <-time.After(2 * time.Second):
		t.Fatal("dump retry backoff ignored shutdown")
	}
}
