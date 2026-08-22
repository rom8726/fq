package replication

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReplicaTrackerHandlesConcurrentRequests(t *testing.T) {
	tracker := NewReplicaTracker()
	var wg sync.WaitGroup

	for replica := 0; replica < 8; replica++ {
		replicaID := fmt.Sprintf("replica-%d", replica)
		for ack := 0; ack < 100; ack++ {
			wg.Add(1)
			go func(replicaID string, ack int) {
				defer wg.Done()

				tracker.Ack(ReplicaCursor{
					ReplicaID:       replicaID,
					LastSegmentName: fmt.Sprintf("wal_%d.log", ack),
					SegmentOffset:   int64(ack),
					LastAppliedLSN:  uint64(ack),
					UpdatedAt:       time.Now(),
				})
				_ = tracker.List()
			}(replicaID, ack)
		}
	}

	wg.Wait()

	cursors := tracker.List()
	require.Len(t, cursors, 8)
	for _, cursor := range cursors {
		require.NotEmpty(t, cursor.ReplicaID)
		require.NotZero(t, cursor.UpdatedAt)
	}
}

func TestReplicaTrackerMinLastAppliedLSN(t *testing.T) {
	tracker := NewReplicaTracker()

	_, ok := tracker.MinLastAppliedLSN()
	require.False(t, ok)

	tracker.Ack(ReplicaCursor{
		ReplicaID:      "replica-1",
		LastAppliedLSN: 100,
		UpdatedAt:      time.Now(),
	})
	tracker.Ack(ReplicaCursor{
		ReplicaID:      "replica-2",
		LastAppliedLSN: 40,
		UpdatedAt:      time.Now(),
	})
	tracker.Ack(ReplicaCursor{
		ReplicaID:      "replica-3",
		LastAppliedLSN: 70,
		UpdatedAt:      time.Now(),
	})

	minLSN, ok := tracker.MinLastAppliedLSN()
	require.True(t, ok)
	require.Equal(t, uint64(40), minLSN)
}
