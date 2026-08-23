package wal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database/compute"
)

func TestReadLogsTruncatesIncompletePayloadTailInLastSegment(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	validBatch := mustEncodeLogs(t, []*LogData{testLogData(1)})
	incompleteBatch := mustEncodeLogs(t, []*LogData{testLogData(2)})
	incompleteBatch = incompleteBatch[:len(incompleteBatch)-2]
	segmentPath := writeWALSegment(t, dir, "wal_1000.log", appendCopy(validBatch, incompleteBatch...))

	logger := zerolog.Nop()
	reader := NewFSReader(dir, &logger)

	logs, err := reader.ReadLogs(context.Background())
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(1), logs[0].LSN)

	stat, err := os.Stat(segmentPath)
	require.NoError(t, err)
	require.Equal(t, int64(len(validBatch)), stat.Size())

	logs, err = reader.ReadSegment(context.Background(), segmentPath)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(1), logs[0].LSN)
}

func TestReadLogsTruncatesIncompleteHeaderTailInLastSegment(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	validBatch := mustEncodeLogs(t, []*LogData{testLogData(1)})
	segmentPath := writeWALSegment(t, dir, "wal_1000.log", appendCopy(validBatch, 0x00, 0x01))

	logger := zerolog.Nop()
	reader := NewFSReader(dir, &logger)

	logs, err := reader.ReadLogs(context.Background())
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(1), logs[0].LSN)

	stat, err := os.Stat(segmentPath)
	require.NoError(t, err)
	require.Equal(t, int64(len(validBatch)), stat.Size())
}

func TestReadLogsRejectsTruncatedNonLastSegment(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	incompleteBatch := mustEncodeLogs(t, []*LogData{testLogData(1)})
	incompleteBatch = incompleteBatch[:len(incompleteBatch)-1]
	writeWALSegment(t, dir, "wal_1000.log", incompleteBatch)
	writeWALSegment(t, dir, "wal_2000.log", mustEncodeLogs(t, []*LogData{testLogData(2)}))

	logger := zerolog.Nop()
	reader := NewFSReader(dir, &logger)

	_, err := reader.ReadLogs(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "truncated WAL batch data")
}

func TestReadLogsRejectsCorruptedCompleteBatchInLastSegment(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	validBatch := mustEncodeLogs(t, []*LogData{testLogData(1)})
	corruptedBatch := appendCopy(uint32ToBytes(2), 0xff, 0xff)
	writeWALSegment(t, dir, "wal_1000.log", appendCopy(validBatch, corruptedBatch...))

	logger := zerolog.Nop()
	reader := NewFSReader(dir, &logger)

	_, err := reader.ReadLogs(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to unmarshal WAL batch")
}

func TestReadSegmentRejectsTruncatedTail(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	incompleteBatch := mustEncodeLogs(t, []*LogData{testLogData(1)})
	incompleteBatch = incompleteBatch[:len(incompleteBatch)-1]
	segmentPath := writeWALSegment(t, dir, "wal_1000.log", incompleteBatch)

	logger := zerolog.Nop()
	reader := NewFSReader(dir, &logger)

	_, err := reader.ReadSegment(context.Background(), segmentPath)
	require.Error(t, err)
	require.Contains(t, err.Error(), "truncated WAL batch data")
}

func testLogData(lsn uint64) *LogData {
	return &LogData{
		LSN:       lsn,
		CommandId: uint32(compute.IncrCommandID),
		Arguments: []string{"key", "60", "1"},
	}
}

func mustEncodeLogs(t *testing.T, logs []*LogData) []byte {
	t.Helper()

	data, err := encodeLogs(logs)
	require.NoError(t, err)

	return data
}

func writeWALSegment(t *testing.T, dir, name string, data []byte) string {
	t.Helper()

	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, data, 0o600))

	return path
}

func appendCopy(data []byte, extra ...byte) []byte {
	result := make([]byte, 0, len(data)+len(extra))
	result = append(result, data...)
	result = append(result, extra...)

	return result
}
