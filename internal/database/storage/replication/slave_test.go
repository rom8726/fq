package replication

import (
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

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
