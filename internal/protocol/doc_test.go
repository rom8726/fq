package protocol_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/protocol"
)

var codeRowPattern = regexp.MustCompile(`(?m)^\| (\d{4}) \| ([^|]+?) \|`)

func TestProtocolDocMatchesCodes(t *testing.T) {
	t.Parallel()

	content, err := os.ReadFile(filepath.Join("..", "..", "docs", "protocol.md"))
	require.NoError(t, err)

	documented := make(map[protocol.Code]string)
	for _, match := range codeRowPattern.FindAllStringSubmatch(string(content), -1) {
		value, parseErr := strconv.ParseUint(match[1], 10, 16)
		require.NoError(t, parseErr)

		documented[protocol.Code(value)] = match[2]
	}

	for _, info := range protocol.AllCodes() {
		message, found := documented[info.Code]
		require.Truef(t, found, "code %d (%s) is missing from docs/protocol.md", info.Code, info.Name)
		require.Equalf(t, info.Message, message,
			"code %d has a different message in docs/protocol.md", info.Code)
	}

	require.Lenf(t, documented, len(protocol.AllCodes()),
		"docs/protocol.md documents codes that do not exist in the registry")
}
