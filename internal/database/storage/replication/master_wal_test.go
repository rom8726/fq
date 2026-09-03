package replication

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database/compute"
	"github.com/fq-db/fq/internal/database/storage/format"
	"github.com/fq-db/fq/internal/database/storage/wal"
)

func TestReadCompleteWALChunkStopsAtFrameBoundary(t *testing.T) {
	segment := testWALSegment("first", "second")
	segmentPath := filepath.Join(t.TempDir(), "wal_1.log")
	require.NoError(t, os.WriteFile(segmentPath, segment, 0o644))

	firstEnd := format.HeaderSize + len(testWALBatch([]byte("first")))

	data, nextOffset, err := readCompleteWALChunk(segmentPath, 0, int64(firstEnd+2))

	require.NoError(t, err)
	require.Equal(t, segment[:firstEnd], data)
	require.Equal(t, int64(firstEnd), nextOffset)
}

func TestReadCompleteWALChunkFromOffsetHasNoHeader(t *testing.T) {
	segment := testWALSegment("first", "second")
	segmentPath := filepath.Join(t.TempDir(), "wal_1.log")
	require.NoError(t, os.WriteFile(segmentPath, segment, 0o644))

	firstEnd := format.HeaderSize + len(testWALBatch([]byte("first")))

	data, nextOffset, err := readCompleteWALChunk(segmentPath, int64(firstEnd), 1024)

	require.NoError(t, err)
	require.Equal(t, segment[firstEnd:], data)
	require.Equal(t, int64(len(segment)), nextOffset)
}

func TestReadCompleteWALChunkReturnsNothingForHeaderOnlySegment(t *testing.T) {
	segment := testWALSegment()
	segmentPath := filepath.Join(t.TempDir(), "wal_1.log")
	require.NoError(t, os.WriteFile(segmentPath, segment, 0o644))

	data, nextOffset, err := readCompleteWALChunk(segmentPath, 0, 1024)

	require.NoError(t, err)
	require.Empty(t, data)
	require.Zero(t, nextOffset)
}

func TestMasterSynchronizeWALReturnsChunkFromOffset(t *testing.T) {
	directory := t.TempDir()
	segment := testWALSegment("first", "second")
	require.NoError(t, os.WriteFile(filepath.Join(directory, "wal_1.log"), segment, 0o644))
	logger := zerolog.Nop()
	master := &Master{walDirectory: directory, logger: &logger}

	firstEnd := format.HeaderSize + len(testWALBatch([]byte("first")))

	response := master.synchronizeWAL(WALRequest{
		ReplicaID:       "replica-1",
		LastSegmentName: "wal_1.log",
		SegmentOffset:   int64(firstEnd),
	})

	require.True(t, response.Succeed)
	require.Equal(t, "wal_1.log", response.SegmentName)
	require.Equal(t, int64(firstEnd), response.SegmentOffset)
	require.Equal(t, int64(len(segment)), response.NextSegmentOffset)
	require.Equal(t, segment[firstEnd:], response.SegmentData)
}

func TestMasterChunksSurviveWALReader(t *testing.T) {
	directory := t.TempDir()
	logger := zerolog.Nop()

	writer := wal.NewFSWriter(directory, 0, format.Compression{}, &logger)
	batch := []wal.Log{wal.NewLog(1, compute.IncrCommandID, []string{"key", "60"})}
	writer.WriteBatch(batch)
	for _, record := range batch {
		result := record.Result()
		require.NoError(t, result.Get())
	}
	require.NoError(t, writer.Close())

	segmentName, err := wal.SegmentLast(directory)
	require.NoError(t, err)
	require.NotEmpty(t, segmentName)

	master := &Master{walDirectory: directory, logger: &logger}
	response := master.synchronizeWAL(WALRequest{ReplicaID: "replica-1", LastSegmentName: segmentName})
	require.True(t, response.Succeed)
	require.NotEmpty(t, response.SegmentData)
	require.Zero(t, response.SegmentOffset)

	reader := wal.NewFSReader(directory, &logger)
	logs, err := reader.ReadSegmentData(context.Background(), response.SegmentData, response.SegmentOffset == 0, 0)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(1), logs[0].LSN)
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
	return format.AppendFrame(nil, payload)
}

func testWALSegment(payloads ...string) []byte {
	data := format.AppendHeader(nil, format.MagicWAL, 1)
	for _, payload := range payloads {
		data = format.AppendFrame(data, []byte(payload))
	}

	return data
}
