package replication

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSaveWALSegmentReplacesExistingFileWithoutTrailingBytes(t *testing.T) {
	directory := t.TempDir()
	slave := &Slave{walDirectory: directory}
	segmentName := "wal_1.log"
	segmentPath := filepath.Join(directory, segmentName)

	require.NoError(t, os.WriteFile(segmentPath, []byte("old data with stale tail"), 0o644))

	require.NoError(t, slave.saveWALSegment(segmentName, []byte("new")))

	data, err := os.ReadFile(segmentPath)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), data)
}

func TestSaveWALSegmentCreatesDirectoryAndRemovesTempFile(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "wal")
	slave := &Slave{walDirectory: directory}

	require.NoError(t, slave.saveWALSegment("wal_1.log", []byte("segment data")))

	data, err := os.ReadFile(filepath.Join(directory, "wal_1.log"))
	require.NoError(t, err)
	require.Equal(t, []byte("segment data"), data)

	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "wal_1.log", entries[0].Name())
}

func TestSaveWALSegmentRejectsUnsafeSegmentNames(t *testing.T) {
	directory := t.TempDir()
	slave := &Slave{walDirectory: directory}

	for _, segmentName := range []string{
		"",
		"../wal_1.log",
		"nested/wal_1.log",
		`nested\wal_1.log`,
		filepath.Join(directory, "wal_1.log"),
	} {
		t.Run(segmentName, func(t *testing.T) {
			require.Error(t, slave.saveWALSegment(segmentName, []byte("data")))
		})
	}
}
