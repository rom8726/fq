package protocol_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/protocol"
)

var errSample = protocol.NewError(protocol.CodeQuotaNotFound, "quota not found")

func TestErrorImplementsError(t *testing.T) {
	require.EqualError(t, errSample, "quota not found")
}

func TestErrorsIsFindsSentinel(t *testing.T) {
	wrapped := fmt.Errorf("storage: %w", errSample)
	require.ErrorIs(t, wrapped, errSample)
}

func TestCodeOfUnwrapsThroughFmtErrorf(t *testing.T) {
	wrapped := fmt.Errorf("%w: %d", errSample, 42)

	code, ok := protocol.CodeOf(wrapped)
	require.True(t, ok)
	require.Equal(t, protocol.CodeQuotaNotFound, code)
}

func TestCodeOfReturnsFalseForPlainError(t *testing.T) {
	code, ok := protocol.CodeOf(errors.New("something went wrong"))
	require.False(t, ok)
	require.Equal(t, protocol.Code(0), code)
}

func TestErrorfKeepsCodeAndFormatsMessage(t *testing.T) {
	err := protocol.Errorf(protocol.CodeMessageTooLarge, "message size %d exceeds maximum %d", 8192, 4096)

	code, ok := protocol.CodeOf(err)
	require.True(t, ok)
	require.Equal(t, protocol.CodeMessageTooLarge, code)
	require.EqualError(t, err, "message size 8192 exceeds maximum 4096")
}
