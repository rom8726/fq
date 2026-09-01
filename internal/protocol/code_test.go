package protocol_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/protocol"
)

func TestAllCodesAreUnique(t *testing.T) {
	seen := make(map[protocol.Code]string)
	for _, info := range protocol.AllCodes() {
		previous, duplicate := seen[info.Code]
		require.Falsef(t, duplicate, "code %d used by %s and %s", info.Code, previous, info.Name)
		seen[info.Code] = info.Name
	}
}

func TestAllCodesAreFourDigit(t *testing.T) {
	for _, info := range protocol.AllCodes() {
		require.GreaterOrEqual(t, int(info.Code), 1000, info.Name)
		require.LessOrEqual(t, int(info.Code), 9999, info.Name)
	}
}

func TestCodeCategory(t *testing.T) {
	require.Equal(t, 1, protocol.CodeInvalidCommand.Category())
	require.Equal(t, 4, protocol.CodeQuotaNotFound.Category())
	require.Equal(t, 9, protocol.CodeInternal.Category())
}

func TestCodeStringReturnsRegisteredMessage(t *testing.T) {
	require.Equal(t, "quota not found", protocol.CodeQuotaNotFound.String())
	require.Equal(t, "internal error", protocol.Code(7777).String())
}
