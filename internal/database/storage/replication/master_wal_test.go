package replication

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestReadCompleteWALChunkStopsAtBatchBoundary(t *testing.T) {
	first := testWALBatch([]byte("first"))
	second := testWALBatch([]byte("second"))
	segmentPath := filepath.Join(t.TempDir(), "wal_1.log")
	require.NoError(t, os.WriteFile(segmentPath, append(append([]byte{}, first...), second...), 0o644))

	data, nextOffset, err := readCompleteWALChunk(segmentPath, 0, int64(len(first)+2))

	require.NoError(t, err)
	require.Equal(t, first, data)
	require.Equal(t, int64(len(first)), nextOffset)
}

func TestMasterSynchronizeWALReturnsChunkFromOffset(t *testing.T) {
	directory := t.TempDir()
	first := testWALBatch([]byte("first"))
	second := testWALBatch([]byte("second"))
	require.NoError(t, os.WriteFile(filepath.Join(directory, "wal_1.log"), append(append([]byte{}, first...), second...), 0o644))
	logger := zerolog.Nop()
	master := &Master{walDirectory: directory, logger: &logger}

	response := master.synchronizeWAL(WALRequest{
		ReplicaID:       "replica-1",
		LastSegmentName: "wal_1.log",
		SegmentOffset:   int64(len(first)),
	})

	require.True(t, response.Succeed)
	require.Equal(t, "wal_1.log", response.SegmentName)
	require.Equal(t, int64(len(first)), response.SegmentOffset)
	require.Equal(t, int64(len(first)+len(second)), response.NextSegmentOffset)
	require.Equal(t, second, response.SegmentData)
}

func TestMasterSynchronizeWALRejectsMissingReplicaID(t *testing.T) {
	logger := zerolog.Nop()
	master := &Master{
		walDirectory: t.TempDir(),
		tracker:      NewReplicaTracker(),
		logger:       &logger,
	}

	response := master.synchronizeWAL(WALRequest{})

	require.False(t, response.Succeed)
	require.Empty(t, master.ReplicaCursors())
}

func TestMasterSynchronizeWALSavesReplicaCursor(t *testing.T) {
	logger := zerolog.Nop()
	master := &Master{
		walDirectory: t.TempDir(),
		tracker:      NewReplicaTracker(),
		logger:       &logger,
	}

	response := master.synchronizeWAL(WALRequest{
		ReplicaID:       "replica-1",
		LastSegmentName: "wal_1.log",
		SegmentOffset:   128,
		LastAppliedLSN:  42,
	})

	require.True(t, response.Succeed)
	cursors := master.ReplicaCursors()
	require.Len(t, cursors, 1)
	require.Equal(t, "replica-1", cursors[0].ReplicaID)
	require.Equal(t, "wal_1.log", cursors[0].LastSegmentName)
	require.Equal(t, int64(128), cursors[0].SegmentOffset)
	require.Equal(t, uint64(42), cursors[0].LastAppliedLSN)
	require.NotZero(t, cursors[0].UpdatedAt)
}

func TestMasterSynchronizeWALUpdatesExistingReplicaCursor(t *testing.T) {
	logger := zerolog.Nop()
	master := &Master{
		walDirectory: t.TempDir(),
		tracker:      NewReplicaTracker(),
		logger:       &logger,
	}

	first := WALRequest{
		ReplicaID:       "replica-1",
		LastSegmentName: "wal_1.log",
		SegmentOffset:   128,
		LastAppliedLSN:  42,
	}
	second := WALRequest{
		ReplicaID:       "replica-1",
		LastSegmentName: "wal_2.log",
		SegmentOffset:   256,
		LastAppliedLSN:  84,
	}

	require.True(t, master.synchronizeWAL(first).Succeed)
	require.True(t, master.synchronizeWAL(second).Succeed)

	cursors := master.ReplicaCursors()
	require.Len(t, cursors, 1)
	require.Equal(t, "wal_2.log", cursors[0].LastSegmentName)
	require.Equal(t, int64(256), cursors[0].SegmentOffset)
	require.Equal(t, uint64(84), cursors[0].LastAppliedLSN)
}

func TestMasterSynchronizeWALTracksReplicasIndependently(t *testing.T) {
	logger := zerolog.Nop()
	master := &Master{
		walDirectory: t.TempDir(),
		tracker:      NewReplicaTracker(),
		logger:       &logger,
	}

	require.True(t, master.synchronizeWAL(WALRequest{
		ReplicaID:      "replica-1",
		SegmentOffset:  128,
		LastAppliedLSN: 42,
	}).Succeed)
	require.True(t, master.synchronizeWAL(WALRequest{
		ReplicaID:      "replica-2",
		SegmentOffset:  256,
		LastAppliedLSN: 84,
	}).Succeed)

	cursors := master.ReplicaCursors()
	require.Len(t, cursors, 2)
	require.Equal(t, "replica-1", cursors[0].ReplicaID)
	require.Equal(t, int64(128), cursors[0].SegmentOffset)
	require.Equal(t, uint64(42), cursors[0].LastAppliedLSN)
	require.Equal(t, "replica-2", cursors[1].ReplicaID)
	require.Equal(t, int64(256), cursors[1].SegmentOffset)
	require.Equal(t, uint64(84), cursors[1].LastAppliedLSN)
}

func testWALBatch(payload []byte) []byte {
	data := make([]byte, walBatchSizeHeaderSize+len(payload))
	binary.BigEndian.PutUint32(data[:walBatchSizeHeaderSize], uint32(len(payload)))
	copy(data[walBatchSizeHeaderSize:], payload)

	return data
}
