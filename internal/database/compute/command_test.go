package compute_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database/compute"
)

func TestCommandNameToCommandID(t *testing.T) {
	require.Equal(t, compute.IncrCommandID, compute.CommandNameToCommandID("INCR"))
	require.Equal(t, compute.GetCommandID, compute.CommandNameToCommandID("GET"))
	require.Equal(t, compute.DelCommandID, compute.CommandNameToCommandID("DEL"))
	require.Equal(t, compute.MsgSizeCommandID, compute.CommandNameToCommandID("MSGSIZE"))
	require.Equal(t, compute.StreamCommandID, compute.CommandNameToCommandID("STREAM"))
	require.Equal(t, compute.PStreamCommandID, compute.CommandNameToCommandID("PSTREAM"))
	require.Equal(t, compute.QStreamCommandID, compute.CommandNameToCommandID("QSTREAM"))
	require.Equal(t, compute.QPStreamCommandID, compute.CommandNameToCommandID("QPSTREAM"))
	require.Equal(t, compute.RLimitCommandID, compute.CommandNameToCommandID("RLIMIT"))
	require.Equal(t, compute.QuotaCommandID, compute.CommandNameToCommandID("QUOTA"))
	require.Equal(t, compute.UnknownCommandID, compute.CommandNameToCommandID("TRUNCATE"))
}

func TestPersistentCommandIDsDoNotDrift(t *testing.T) {
	require.Equal(t, compute.CommandID(9), compute.RLimitCommandID)
	require.Equal(t, compute.CommandID(10), compute.RLimitSlidingWindowCommandID)
	require.Equal(t, compute.CommandID(11), compute.RLimitTokenBucketCommandID)
	require.Equal(t, compute.CommandID(12), compute.QuotaCommandID)
	require.Equal(t, compute.CommandID(13), compute.QuotaAcquireCommandID)
	require.Equal(t, compute.CommandID(14), compute.QuotaReleaseCommandID)
	require.Equal(t, compute.CommandID(15), compute.QuotaDeleteCommandID)
	require.Equal(t, compute.CommandID(16), compute.QStreamCommandID)
	require.Equal(t, compute.CommandID(17), compute.QPStreamCommandID)
}
