package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSegmentHelpersIgnoreMetadataFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "wal_1000.log"), nil, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "wal_1000.log.meta"), nil, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "wal_2000.log"), nil, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "wal_2000.log.meta"), nil, 0o600))

	filename, err := SegmentUpperBound(dir, "wal_1000.log")
	require.NoError(t, err)
	require.Equal(t, "wal_2000.log", filename)

	filename, err = SegmentLast(dir)
	require.NoError(t, err)
	require.Equal(t, "wal_2000.log", filename)
}

//
//func TestSegmentUpperBound(t *testing.T) {
//	t.Parallel()
//
//	filename, err := SegmentUpperBound("test_data", "wal_0.log")
//	require.NoError(t, err)
//	require.Equal(t, "wal_1000.log", filename)
//
//	filename, err = SegmentUpperBound("test_data", "wal_1000.log")
//	require.NoError(t, err)
//	require.Equal(t, "wal_2000.log", filename)
//
//	filename, err = SegmentUpperBound("test_data", "wal_2000.log")
//	require.NoError(t, err)
//	require.Equal(t, "wal_3000.log", filename)
//
//	filename, err = SegmentUpperBound("test_data", "wal_3000.log")
//	require.NoError(t, err)
//	require.Equal(t, "", filename)
//}
//
//func TestSegmentLast(t *testing.T) {
//	t.Parallel()
//
//	filename, err := SegmentLast("test_data")
//	require.NoError(t, err)
//	require.Equal(t, "wal_3000.log", filename)
//}
