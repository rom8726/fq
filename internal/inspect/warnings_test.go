package inspect

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/config"
)

func intPtr(v int) *int         { return &v }
func strPtr(v string) *string   { return &v }
func f64Ptr(v float64) *float64 { return &v }

func TestWALQueueWarnings(t *testing.T) {
	require.Nil(t, walQueueWarnings(nil))
	require.Nil(t, walQueueWarnings(&WALInfo{}))
	require.Nil(t, walQueueWarnings(&WALInfo{QueueDepth: intPtr(1), QueueCapacity: intPtr(0)}))
	require.Nil(t, walQueueWarnings(&WALInfo{QueueDepth: intPtr(1), QueueCapacity: intPtr(10)}))

	warnings := walQueueWarnings(&WALInfo{QueueDepth: intPtr(8), QueueCapacity: intPtr(10)})
	require.Len(t, warnings, 1)
	require.Equal(t, "wal_queue_pressure", warnings[0].Code)
	require.Equal(t, severityWarn, warnings[0].Severity)
}

func TestReplWarnings(t *testing.T) {
	require.Nil(t, replWarnings(nil))
	require.Nil(t, replWarnings(&ReplInfo{}))

	warnings := replWarnings(&ReplInfo{Replicas: []ReplicaInfo{
		{ReplicaID: "stale-one", Stale: true, LagSec: 100},
		{ReplicaID: "laggy", LagSec: 10},
		{ReplicaID: "healthy", LagSec: 0.1},
	}})
	require.Len(t, warnings, 2)
	require.Equal(t, "replica_stale", warnings[0].Code)
	require.Equal(t, severityCrit, warnings[0].Severity)
	require.Equal(t, "replica_lag", warnings[1].Code)
	require.Equal(t, severityWarn, warnings[1].Severity)
}

func TestStreamWarnings(t *testing.T) {
	require.Nil(t, streamWarnings(nil))
	require.Nil(t, streamWarnings(&StreamsInfo{}))

	warnings := streamWarnings(&StreamsInfo{
		LimitSubscribers: []SubscriberInfo{{Dropped: 0}, {Dropped: 3}},
		QuotaSubscribers: []SubscriberInfo{{Dropped: 5}},
	})
	require.Len(t, warnings, 2)
	for _, w := range warnings {
		require.Equal(t, "stream_subscriber_dropping_events", w.Code)
	}
}

func TestDumpWarnings(t *testing.T) {
	require.Nil(t, dumpWarnings(nil))
	require.Nil(t, dumpWarnings(&DumpInfo{Enabled: false}))
	require.Nil(t, dumpWarnings(&DumpInfo{Enabled: true}))

	warnings := dumpWarnings(&DumpInfo{Enabled: true, LastDumpError: strPtr("disk full")})
	require.Len(t, warnings, 1)
	require.Equal(t, "dump_failed", warnings[0].Code)

	recent := time.Now().Unix()
	warnings = dumpWarnings(&DumpInfo{
		Enabled:     true,
		LastDumpAt:  &recent,
		IntervalSec: f64Ptr(3600),
	})
	require.Empty(t, warnings)

	overdue := time.Now().Add(-time.Hour * 24).Unix()
	warnings = dumpWarnings(&DumpInfo{
		Enabled:     true,
		LastDumpAt:  &overdue,
		IntervalSec: f64Ptr(60),
	})
	require.Len(t, warnings, 1)
	require.Equal(t, "dump_overdue", warnings[0].Code)
}

func TestDurabilityWarnings(t *testing.T) {
	require.Nil(t, durabilityWarnings(nil, nil))

	warnings := durabilityWarnings(&PersistenceInfo{Mode: config.PersistenceModeMemory}, nil)
	require.Len(t, warnings, 1)
	require.Equal(t, "no_durability", warnings[0].Code)

	warnings = durabilityWarnings(
		&PersistenceInfo{Mode: config.PersistenceModeWALAndDump},
		&WALInfo{SyncCommit: strPtr(config.WALSyncCommitOff)},
	)
	require.Len(t, warnings, 1)
	require.Equal(t, "async_wal", warnings[0].Code)

	warnings = durabilityWarnings(
		&PersistenceInfo{Mode: config.PersistenceModeWALAndDump},
		&WALInfo{SyncCommit: strPtr(config.WALSyncCommitOn)},
	)
	require.Empty(t, warnings)

	warnings = durabilityWarnings(&PersistenceInfo{Mode: config.PersistenceModeWALAndDump}, nil)
	require.Empty(t, warnings)
}

func TestBuildWarningsAggregatesAllCategories(t *testing.T) {
	report := &Report{
		WAL:         &WALInfo{QueueDepth: intPtr(9), QueueCapacity: intPtr(10)},
		Repl:        &ReplInfo{Replicas: []ReplicaInfo{{ReplicaID: "r1", Stale: true}}},
		Streams:     &StreamsInfo{LimitSubscribers: []SubscriberInfo{{Dropped: 1}}},
		Dump:        &DumpInfo{Enabled: true, LastDumpError: strPtr("boom")},
		Persistence: &PersistenceInfo{Mode: config.PersistenceModeMemory},
	}

	warnings := buildWarnings(report)
	require.Len(t, warnings, 5)
}

func TestReplicaStaleThreshold(t *testing.T) {
	require.Equal(t, minReplicaStaleInterval, replicaStaleThreshold(0))
	require.Equal(t, minReplicaStaleInterval, replicaStaleThreshold(-time.Second))
	require.Equal(t, minReplicaStaleInterval, replicaStaleThreshold(time.Second))
	require.Equal(t, 50*time.Second, replicaStaleThreshold(10*time.Second))
}
