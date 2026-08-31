package wal

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database/storage/format"
	"github.com/fq-db/fq/internal/database/storage/format/formattest"
)

func TestSegmentMetadataRoundTrip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	segmentPath := writeWALSegment(t, dir, "wal_1000.log", mustEncodeLogs(t, []*LogData{testLogData(7)}))
	require.NoError(t, writeSegmentMetadata(segmentPath, segmentMetadata{MaxLSN: 7}))

	meta, err := readSegmentMetadata(segmentPath)
	require.NoError(t, err)
	require.Equal(t, uint64(7), meta.MaxLSN)
}

func TestReadSegmentMetadataRejectsChecksumMismatch(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	segmentPath := writeWALSegment(t, dir, "wal_1000.log", mustEncodeLogs(t, []*LogData{testLogData(7)}))
	require.NoError(t, writeSegmentMetadata(segmentPath, segmentMetadata{MaxLSN: 7}))

	metadataPath := segmentMetadataPath(segmentPath)
	data, err := os.ReadFile(metadataPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(metadataPath, formattest.CorruptPayload(t, data, format.HeaderSize), 0o600))

	_, err = readSegmentMetadata(segmentPath)
	require.ErrorIs(t, err, format.ErrChecksumMismatch)
}

func TestReadLogsAfterFallsBackToScanWhenMetadataIsCorrupted(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	firstPath := writeWALSegment(t, dir, "wal_1000.log", mustEncodeLogs(t, []*LogData{testLogData(1)}))
	writeWALSegment(t, dir, "wal_2000.log", mustEncodeLogs(t, []*LogData{testLogData(2)}))
	require.NoError(t, writeSegmentMetadata(firstPath, segmentMetadata{MaxLSN: 1}))

	metadataPath := segmentMetadataPath(firstPath)
	data, err := os.ReadFile(metadataPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(metadataPath, formattest.CorruptPayload(t, data, format.HeaderSize), 0o600))

	logger := zerolog.Nop()
	reader := NewFSReader(dir, &logger)

	logs, err := reader.ReadLogsAfter(context.Background(), 1)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(2), logs[0].LSN)
}

func TestLastFlushDBLSNRoundTrip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, writeLastFlushDBLSN(dir, 42))

	lsn, err := readLastFlushDBLSN(dir)
	require.NoError(t, err)
	require.Equal(t, uint64(42), lsn)
}

func TestReadLastFlushDBLSNRejectsForeignMagic(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, writeLastFlushDBLSN(dir, 42))

	path := filepath.Join(dir, lastFlushDBLSNFileName)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, formattest.CorruptMagic(t, data), 0o600))

	_, err = readLastFlushDBLSN(dir)
	require.ErrorIs(t, err, format.ErrBadMagic)
}
