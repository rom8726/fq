package wal

import (
	"context"
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
