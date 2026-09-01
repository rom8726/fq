package observability

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestIncAuthFailures(t *testing.T) {
	before := testutil.ToFloat64(authFailuresTotal.WithLabelValues("9000"))
	IncAuthFailures("9000")
	require.Equal(t, before+1, testutil.ToFloat64(authFailuresTotal.WithLabelValues("9000")))
}

func TestIncProtocolError(t *testing.T) {
	before := testutil.ToFloat64(protocolErrorsTotal.WithLabelValues("1234"))
	IncProtocolError(1234)
	require.Equal(t, before+1, testutil.ToFloat64(protocolErrorsTotal.WithLabelValues("1234")))
}

func TestTCPActiveConnectionsIncDec(t *testing.T) {
	before := testutil.ToFloat64(tcpActiveConnections)
	IncTCPActiveConnections()
	require.Equal(t, before+1, testutil.ToFloat64(tcpActiveConnections))
	DecTCPActiveConnections()
	require.Equal(t, before, testutil.ToFloat64(tcpActiveConnections))
}

func TestSetWALQueueDepth(t *testing.T) {
	SetWALQueueDepth(7)
	require.Equal(t, float64(7), testutil.ToFloat64(walQueueDepth))
}

func TestObserveWALFlushLatencyAndBatchSize(t *testing.T) {
	beforeTotal := testutil.ToFloat64(walFlushTotal)

	ObserveWALFlushLatency(250 * time.Millisecond)
	require.Equal(t, beforeTotal+1, testutil.ToFloat64(walFlushTotal))

	require.NotPanics(t, func() {
		ObserveWALFlushBatchSize(16)
	})
}

func TestReplicationGauges(t *testing.T) {
	SetReplicationLagLSN(123)
	require.Equal(t, float64(123), testutil.ToFloat64(replicationLagLSN))

	beforeReconnect := testutil.ToFloat64(replicationReconnectTotal)
	IncReplicationReconnectTotal()
	require.Equal(t, beforeReconnect+1, testutil.ToFloat64(replicationReconnectTotal))

	beforeAttempts := testutil.ToFloat64(replicationReconnectAttemptsTotal)
	IncReplicationReconnectAttemptsTotal()
	require.Equal(t, beforeAttempts+1, testutil.ToFloat64(replicationReconnectAttemptsTotal))

	SetReplicationReplicaLastAppliedLSN("replica-1", 42)
	require.Equal(t, float64(42), testutil.ToFloat64(replicationReplicaLastAppliedLSN.WithLabelValues("replica-1")))

	now := time.Unix(1700000000, 0)
	SetReplicationReplicaLastAckTimestamp("replica-1", now)
	require.Equal(
		t,
		float64(now.Unix()),
		testutil.ToFloat64(replicationReplicaLastAckTimestamp.WithLabelValues("replica-1")),
	)

	SetReplicationKnownReplicas(3)
	require.Equal(t, float64(3), testutil.ToFloat64(replicationKnownReplicas))
}

func TestGetSnapshotReflectsMetricValues(t *testing.T) {
	SetWALQueueDepth(9)
	SetReplicationLagLSN(5)
	SetReplicationKnownReplicas(2)

	snap, err := GetSnapshot()
	require.NoError(t, err)
	require.Equal(t, float64(9), snap.WALQueueDepth)
	require.Equal(t, float64(5), snap.ReplicationLagLSN)
	require.Equal(t, float64(2), snap.ReplicationKnownReplicas)
	require.GreaterOrEqual(t, snap.TCPActiveConnections, float64(0))
}
