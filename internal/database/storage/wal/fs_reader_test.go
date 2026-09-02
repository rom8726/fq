package wal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database/compute"
	"github.com/fq-db/fq/internal/database/storage/format"
	"github.com/fq-db/fq/internal/database/storage/format/formattest"
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
	require.Equal(t, int64(format.HeaderSize+len(validBatch)), stat.Size())

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
	require.Equal(t, int64(format.HeaderSize+len(validBatch)), stat.Size())
}

func TestReadLogsIgnoresSegmentMetadataFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	segmentPath := writeWALSegment(t, dir, "wal_1000.log", mustEncodeLogs(t, []*LogData{testLogData(1)}))
	require.NoError(t, writeSegmentMetadata(segmentPath, segmentMetadata{MaxLSN: 1}))

	logger := zerolog.Nop()
	reader := NewFSReader(dir, &logger)

	logs, err := reader.ReadLogs(context.Background())
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(1), logs[0].LSN)
}

func TestReadLogsCreatesMissingDirectory(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "wal")
	logger := zerolog.Nop()
	reader := NewFSReader(dir, &logger)

	logs, err := reader.ReadLogs(context.Background())
	require.NoError(t, err)
	require.Empty(t, logs)

	stat, err := os.Stat(dir)
	require.NoError(t, err)
	require.True(t, stat.IsDir())
}

func TestReadLogsAfterSkipsSegmentsUsingMetadata(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	firstSegmentPath := writeWALSegment(t, dir, "wal_1000.log", []byte("not decoded"))
	secondSegmentPath := writeWALSegment(t, dir, "wal_2000.log", mustEncodeLogs(t, []*LogData{testLogData(2)}))
	require.NoError(t, writeSegmentMetadata(firstSegmentPath, segmentMetadata{MaxLSN: 1}))
	require.NoError(t, writeSegmentMetadata(secondSegmentPath, segmentMetadata{MaxLSN: 2}))

	logger := zerolog.Nop()
	reader := NewFSReader(dir, &logger)

	logs, err := reader.ReadLogsAfter(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(2), logs[0].LSN)
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
	require.ErrorIs(t, err, format.ErrIncompleteFrame)
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
	require.ErrorIs(t, err, format.ErrIncompleteFrame)
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

	return writeRawWALSegment(t, dir, name, append(segmentHeader(), data...))
}

func writeRawWALSegment(t *testing.T, dir, name string, data []byte) string {
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

func TestReadLogsRejectsChecksumMismatchInLastSegment(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	batch := mustEncodeLogs(t, []*LogData{testLogData(1)})
	segment := formattest.CorruptPayload(t, append(segmentHeader(), batch...), format.HeaderSize)
	writeRawWALSegment(t, dir, "wal_1000.log", segment)

	logger := zerolog.Nop()
	reader := NewFSReader(dir, &logger)

	_, err := reader.ReadLogs(context.Background())
	require.ErrorIs(t, err, format.ErrChecksumMismatch)
}

func TestReadLogsRejectsChecksumMismatchInMiddleSegment(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	batch := mustEncodeLogs(t, []*LogData{testLogData(1)})
	segment := formattest.CorruptPayload(t, append(segmentHeader(), batch...), format.HeaderSize)
	corruptedPath := writeRawWALSegment(t, dir, "wal_1000.log", segment)
	writeWALSegment(t, dir, "wal_2000.log", mustEncodeLogs(t, []*LogData{testLogData(2)}))

	logger := zerolog.Nop()
	reader := NewFSReader(dir, &logger)

	_, err := reader.ReadLogs(context.Background())
	require.ErrorIs(t, err, format.ErrChecksumMismatch)

	stat, err := os.Stat(corruptedPath)
	require.NoError(t, err)
	require.Equal(t, int64(len(segment)), stat.Size())
}

func TestReadLogsRejectsForeignMagic(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	batch := mustEncodeLogs(t, []*LogData{testLogData(1)})
	segment := formattest.CorruptMagic(t, append(segmentHeader(), batch...))
	writeRawWALSegment(t, dir, "wal_1000.log", segment)

	logger := zerolog.Nop()
	reader := NewFSReader(dir, &logger)

	_, err := reader.ReadLogs(context.Background())
	require.ErrorIs(t, err, format.ErrBadMagic)
}

func TestReadLogsRejectsUnknownFormatVersion(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	batch := mustEncodeLogs(t, []*LogData{testLogData(1)})
	segment := formattest.SetVersion(t, append(segmentHeader(), batch...), segmentFormatVersion+1)
	writeRawWALSegment(t, dir, "wal_1000.log", segment)

	logger := zerolog.Nop()
	reader := NewFSReader(dir, &logger)

	_, err := reader.ReadLogs(context.Background())
	require.ErrorIs(t, err, format.ErrUnsupportedVersion)
}

func TestReadLogsSkipsEmptySegment(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeRawWALSegment(t, dir, "wal_1000.log", nil)
	writeWALSegment(t, dir, "wal_2000.log", mustEncodeLogs(t, []*LogData{testLogData(2)}))

	logger := zerolog.Nop()
	reader := NewFSReader(dir, &logger)

	logs, err := reader.ReadLogs(context.Background())
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(2), logs[0].LSN)
}

func TestReadSegmentDataAcceptsChunkWithoutHeader(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	reader := NewFSReader(t.TempDir(), &logger)

	logs, err := reader.ReadSegmentData(
		context.Background(),
		mustEncodeLogs(t, []*LogData{testLogData(5)}),
		false,
	)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(5), logs[0].LSN)
}

func TestReadSegmentDataAcceptsChunkWithHeader(t *testing.T) {
	t.Parallel()

	logger := zerolog.Nop()
	reader := NewFSReader(t.TempDir(), &logger)

	chunk := append(segmentHeader(), mustEncodeLogs(t, []*LogData{testLogData(6)})...)

	logs, err := reader.ReadSegmentData(context.Background(), chunk, true)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(6), logs[0].LSN)
}
