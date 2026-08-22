package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWALRequestEncodesReplicaIDAndLastAppliedLSN(t *testing.T) {
	request := NewWALRequest("replica-1", "wal_1.log", 128, 42)

	data, err := Encode(&request)
	require.NoError(t, err)

	var decoded Request
	require.NoError(t, Decode(&decoded, data))
	require.Equal(t, "replica-1", decoded.ReplicaID)
	require.Equal(t, "wal_1.log", decoded.LastSegmentName)
	require.Equal(t, int64(128), decoded.SegmentOffset)
	require.Equal(t, uint64(42), decoded.LastAppliedLSN)
}
