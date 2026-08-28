package wal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestRemovePastSegmentsKeepsLatestSegment(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeWALSegment(t, dir, "wal_1000.log", mustEncodeLogs(t, []*LogData{testLogData(1)}))
	writeWALSegment(t, dir, "wal_2000.log", mustEncodeLogs(t, []*LogData{testLogData(2)}))
	logger := zerolog.Nop()
	reader := NewFSReader(dir, &logger)
	store := NewWAL(nil, reader, nil, time.Hour, 1, 1, dir, &logger)

	require.NoError(t, store.RemovePastSegments(context.Background(), 10))

	_, err := os.Stat(filepath.Join(dir, "wal_1000.log"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(dir, "wal_2000.log"))
	require.NoError(t, err)
}

func TestRemovePastSegmentsUsesMetadata(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	firstSegmentPath := writeWALSegment(t, dir, "wal_1000.log", []byte("not decoded"))
	secondSegmentPath := writeWALSegment(t, dir, "wal_2000.log", []byte("latest segment"))
	require.NoError(t, writeSegmentMetadata(firstSegmentPath, segmentMetadata{MaxLSN: 9}))
	require.NoError(t, writeSegmentMetadata(secondSegmentPath, segmentMetadata{MaxLSN: 10}))

	logger := zerolog.Nop()
	store := NewWAL(nil, panicSegmentReader{}, nil, time.Hour, 1, 1, dir, &logger)

	require.NoError(t, store.RemovePastSegments(context.Background(), 10))

	_, err := os.Stat(firstSegmentPath)
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(segmentMetadataPath(firstSegmentPath))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(secondSegmentPath)
	require.NoError(t, err)
}

func TestRemovePastSegmentsFallsBackWhenMetadataIsMissing(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeWALSegment(t, dir, "wal_1000.log", mustEncodeLogs(t, []*LogData{testLogData(9)}))
	writeWALSegment(t, dir, "wal_2000.log", mustEncodeLogs(t, []*LogData{testLogData(10)}))
	logger := zerolog.Nop()
	reader := NewFSReader(dir, &logger)
	store := NewWAL(nil, reader, nil, time.Hour, 1, 1, dir, &logger)

	require.NoError(t, store.RemovePastSegments(context.Background(), 10))

	_, err := os.Stat(filepath.Join(dir, "wal_1000.log"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

type panicSegmentReader struct{}

func (panicSegmentReader) ReadLogs(context.Context) ([]*LogData, error) {
	return nil, fmt.Errorf("unexpected ReadLogs call")
}

func (panicSegmentReader) ReadSegment(context.Context, string) ([]*LogData, error) {
	panic("metadata path should not read WAL segment data")
}
