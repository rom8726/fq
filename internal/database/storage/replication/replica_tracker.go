package replication

import (
	"sort"
	"sync"
	"time"
)

type ReplicaCursor struct {
	ReplicaID       string
	LastSegmentName string
	SegmentOffset   int64
	LastAppliedLSN  uint64
	UpdatedAt       time.Time
}

type ReplicaTracker struct {
	mutex   sync.RWMutex
	cursors map[string]ReplicaCursor
}

func NewReplicaTracker() *ReplicaTracker {
	return &ReplicaTracker{
		cursors: make(map[string]ReplicaCursor),
	}
}

func (t *ReplicaTracker) Ack(cursor ReplicaCursor) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.cursors[cursor.ReplicaID] = cursor
}

func (t *ReplicaTracker) List() []ReplicaCursor {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	result := make([]ReplicaCursor, 0, len(t.cursors))
	for _, cursor := range t.cursors {
		result = append(result, cursor)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ReplicaID < result[j].ReplicaID
	})

	return result
}

func (t *ReplicaTracker) MinLastAppliedLSN() (uint64, bool) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	if len(t.cursors) == 0 {
		return 0, false
	}

	var minLSN uint64
	first := true
	for _, cursor := range t.cursors {
		if first || cursor.LastAppliedLSN < minLSN {
			minLSN = cursor.LastAppliedLSN
			first = false
		}
	}

	return minLSN, true
}
