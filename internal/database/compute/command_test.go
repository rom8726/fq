package compute_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"fq/internal/database/compute"
)

func TestCommandNameToCommandID(t *testing.T) {
	require.Equal(t, compute.IncrCommandID, compute.CommandNameToCommandID("INCR"))
	require.Equal(t, compute.GetCommandID, compute.CommandNameToCommandID("GET"))
	require.Equal(t, compute.DelCommandID, compute.CommandNameToCommandID("DEL"))
	require.Equal(t, compute.MsgSizeCommandID, compute.CommandNameToCommandID("MSGSIZE"))
	require.Equal(t, compute.UnknownCommandID, compute.CommandNameToCommandID("TRUNCATE"))
}
