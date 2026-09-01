package inspect_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database/storage/replication"
	"github.com/fq-db/fq/internal/inspect"
	"github.com/fq-db/fq/internal/protocol"
)

func TestReportCarriesProtocolVersions(t *testing.T) {
	t.Parallel()

	report := inspect.Report{
		Instance: &inspect.InstanceInfo{ProtocolVersion: int(protocol.CurrentVersion)},
		Repl: &inspect.ReplInfo{
			Role:            "slave",
			ProtocolVersion: int(replication.ProtocolVersion),
			Slave:           &inspect.SlaveInfo{LastErrorCode: int(protocol.CodeUnsupportedVersion)},
		},
	}

	data, err := json.Marshal(report)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(data, &decoded))

	instance, ok := decoded["instance"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, float64(1), instance["protocol_version"])

	repl, ok := decoded["repl"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, float64(1), repl["protocol_version"])

	slave, ok := repl["slave"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, float64(protocol.CodeUnsupportedVersion), slave["last_error_code"])
}
