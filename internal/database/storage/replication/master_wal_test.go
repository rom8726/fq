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
		LastSegmentName: "wal_1.log",
		SegmentOffset:   int64(len(first)),
	})

	require.True(t, response.Succeed)
	require.Equal(t, "wal_1.log", response.SegmentName)
	require.Equal(t, int64(len(first)), response.SegmentOffset)
	require.Equal(t, int64(len(first)+len(second)), response.NextSegmentOffset)
	require.Equal(t, second, response.SegmentData)
}

func testWALBatch(payload []byte) []byte {
	data := make([]byte, walBatchSizeHeaderSize+len(payload))
	binary.BigEndian.PutUint32(data[:walBatchSizeHeaderSize], uint32(len(payload)))
	copy(data[walBatchSizeHeaderSize:], payload)

	return data
}
