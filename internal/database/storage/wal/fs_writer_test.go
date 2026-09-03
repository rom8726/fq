package wal

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database/compute"
	"github.com/fq-db/fq/internal/database/storage/format"
)

const testWALDirectory = "/tmp/fq_wal_test_data"

func TestMain(m *testing.M) {
	if err := os.RemoveAll(testWALDirectory); err != nil {
		log.Fatal(err)
	}

	if err := os.MkdirAll(testWALDirectory, os.ModePerm); err != nil {
		log.Fatal(err)
	}

	code := m.Run()
	if err := os.RemoveAll(testWALDirectory); err != nil {
		log.Fatal(err)
	}

	os.Exit(code)
}

func TestBatchWritingToWALSegment(t *testing.T) {
	maxSegmentSize := 100 << 10
	logger := zerolog.Nop()
	fsWriter := NewFSWriter(testWALDirectory, maxSegmentSize, format.Compression{}, &logger)

	originalNow := now
	defer func() {
		now = originalNow
	}()

	batch := []Log{
		NewLog(1, compute.IncrCommandID, []string{"key_1", "60"}),
		NewLog(2, compute.IncrCommandID, []string{"key_2", "60"}),
		NewLog(3, compute.IncrCommandID, []string{"key_3", "60"}),
	}

	now = func() time.Time {
		return time.Unix(1, 0)
	}

	fsWriter.WriteBatch(batch)
	for _, record := range batch {
		err := record.Result()
		require.NoError(t, err.Get())
	}

	stat, err := os.Stat(testWALDirectory + "/wal_1000.log")
	require.NoError(t, err)
	require.NotZero(t, stat.Size())

	require.NoError(t, fsWriter.Close())
	meta, err := readSegmentMetadata(testWALDirectory + "/wal_1000.log")
	require.NoError(t, err)
	require.Equal(t, uint64(3), meta.MaxLSN)
}

func TestWALSegmentsRotation(t *testing.T) {
	maxSegmentSize := 10
	logger := zerolog.Nop()
	fsWriter := NewFSWriter(testWALDirectory, maxSegmentSize, format.Compression{}, &logger)
	defer func() {
		require.NoError(t, fsWriter.Close())
	}()

	originalNow := now
	defer func() {
		now = originalNow
	}()

	batch := []Log{
		NewLog(4, compute.IncrCommandID, []string{"key_4", "60"}),
		NewLog(5, compute.IncrCommandID, []string{"key_5", "60"}),
		NewLog(6, compute.IncrCommandID, []string{"key_6", "60"}),
	}

	now = func() time.Time {
		return time.Unix(2, 0)
	}

	fsWriter.WriteBatch(batch)
	for _, record := range batch {
		err := record.Result()
		require.NoError(t, err.Get())
	}

	batch = []Log{
		NewLog(7, compute.IncrCommandID, []string{"key_7", "60"}),
		NewLog(8, compute.IncrCommandID, []string{"key_8", "60"}),
		NewLog(9, compute.IncrCommandID, []string{"key_9", "60"}),
	}

	now = func() time.Time {
		return time.Unix(3, 0)
	}

	fsWriter.WriteBatch(batch)
	for _, record := range batch {
		err := record.Result()
		require.NoError(t, err.Get())
	}

	stat, err := os.Stat(testWALDirectory + "/wal_2000.log")
	require.NoError(t, err)
	require.NotZero(t, stat.Size())
	meta, err := readSegmentMetadata(testWALDirectory + "/wal_2000.log")
	require.NoError(t, err)
	require.Equal(t, uint64(6), meta.MaxLSN)

	stat, err = os.Stat(testWALDirectory + "/wal_3000.log")
	require.NoError(t, err)
	require.NotZero(t, stat.Size())
}

func TestWALSegmentRotatesBeforeNextBatchExceedsLimit(t *testing.T) {
	logger := zerolog.Nop()
	fsWriter := NewFSWriter(t.TempDir(), 100<<10, format.Compression{}, &logger)
	defer func() {
		require.NoError(t, fsWriter.Close())
	}()

	originalNow := now
	now = func() time.Time {
		return time.Unix(10, 0)
	}
	defer func() {
		now = originalNow
	}()

	first := []Log{
		NewLog(10, compute.IncrCommandID, []string{"key_10", "60"}),
	}
	fsWriter.WriteBatch(first)
	for _, record := range first {
		err := record.Result()
		require.NoError(t, err.Get())
	}

	firstSegment := filepath.Join(fsWriter.directory, "wal_10000.log")
	stat, err := os.Stat(firstSegment)
	require.NoError(t, err)
	require.NotZero(t, stat.Size())

	fsWriter.maxSegmentSize = int(stat.Size())
	now = func() time.Time {
		return time.Unix(11, 0)
	}

	second := []Log{
		NewLog(11, compute.IncrCommandID, []string{"key_11", "60"}),
	}
	fsWriter.WriteBatch(second)
	for _, record := range second {
		err := record.Result()
		require.NoError(t, err.Get())
	}

	_, err = os.Stat(filepath.Join(fsWriter.directory, "wal_11000.log"))
	require.NoError(t, err)
}

func TestWALSegmentNamesRemainUniqueWithinSameMillisecond(t *testing.T) {
	logger := zerolog.Nop()
	fsWriter := NewFSWriter(t.TempDir(), 100<<10, format.Compression{}, &logger)
	defer func() {
		require.NoError(t, fsWriter.Close())
	}()

	originalNow := now
	now = func() time.Time {
		return time.Unix(12, 0)
	}
	defer func() {
		now = originalNow
	}()

	first := []Log{
		NewLog(12, compute.IncrCommandID, []string{"key_12", "60"}),
	}
	fsWriter.WriteBatch(first)
	for _, record := range first {
		err := record.Result()
		require.NoError(t, err.Get())
	}

	stat, err := os.Stat(filepath.Join(fsWriter.directory, "wal_12000.log"))
	require.NoError(t, err)
	require.NotZero(t, stat.Size())

	fsWriter.maxSegmentSize = int(stat.Size())
	second := []Log{
		NewLog(13, compute.IncrCommandID, []string{"key_13", "60"}),
	}
	fsWriter.WriteBatch(second)
	for _, record := range second {
		err := record.Result()
		require.NoError(t, err.Get())
	}

	_, err = os.Stat(filepath.Join(fsWriter.directory, "wal_12000_1.log"))
	require.NoError(t, err)
}

func TestWALSegmentSyncIsSkippedWhenAlreadySynced(t *testing.T) {
	logger := zerolog.Nop()
	fsWriter := NewFSWriter(t.TempDir(), 100<<10, format.Compression{}, &logger)
	defer func() {
		require.NoError(t, fsWriter.Close())
	}()

	require.NoError(t, fsWriter.rotateSegment())
	require.NoError(t, fsWriter.writeBytes([]byte("dirty data")))

	require.NoError(t, fsWriter.syncSegment())
	require.Equal(t, fsWriter.segmentSize, fsWriter.syncedSegmentSize)

	syncedSize := fsWriter.syncedSegmentSize
	require.NoError(t, fsWriter.syncSegment())
	require.Equal(t, syncedSize, fsWriter.syncedSegmentSize)
}

func TestWALSegmentCloseIsIdempotentAndRejectsWrites(t *testing.T) {
	logger := zerolog.Nop()
	fsWriter := NewFSWriter(t.TempDir(), 100<<10, format.Compression{}, &logger)

	originalNow := now
	now = func() time.Time {
		return time.Unix(14, 0)
	}
	defer func() {
		now = originalNow
	}()

	first := []Log{
		NewLog(14, compute.IncrCommandID, []string{"key_14", "60"}),
	}
	fsWriter.WriteBatch(first)
	for _, record := range first {
		err := record.Result()
		require.NoError(t, err.Get())
	}

	require.NoError(t, fsWriter.Close())
	require.NoError(t, fsWriter.Close())
	require.Nil(t, fsWriter.segment)

	second := []Log{
		NewLog(15, compute.IncrCommandID, []string{"key_15", "60"}),
	}
	fsWriter.WriteBatch(second)
	for _, record := range second {
		err := record.Result()
		require.ErrorIs(t, err.Get(), errFSWriterClosed)
	}
}

func TestWALSegmentTruncateRemovesFilesAndAllowsNewWrites(t *testing.T) {
	logger := zerolog.Nop()
	dir := t.TempDir()
	fsWriter := NewFSWriter(dir, 100<<10, format.Compression{}, &logger)
	defer func() {
		require.NoError(t, fsWriter.Close())
	}()

	originalNow := now
	now = func() time.Time {
		return time.Unix(20, 0)
	}
	defer func() {
		now = originalNow
	}()

	first := []Log{
		NewLog(20, compute.IncrCommandID, []string{"key_20", "60"}),
	}
	fsWriter.WriteBatch(first)
	for _, record := range first {
		err := record.Result()
		require.NoError(t, err.Get())
	}

	require.NoError(t, writeLastFlushDBLSN(dir, 20))
	require.NoError(t, fsWriter.Truncate())

	files, err := filepath.Glob(filepath.Join(dir, "wal_*.log*"))
	require.NoError(t, err)
	require.Empty(t, files)
	_, err = os.Stat(filepath.Join(dir, lastFlushDBLSNFileName))
	require.ErrorIs(t, err, os.ErrNotExist)

	now = func() time.Time {
		return time.Unix(21, 0)
	}
	second := []Log{
		NewLog(21, compute.IncrCommandID, []string{"key_21", "60"}),
	}
	fsWriter.WriteBatch(second)
	for _, record := range second {
		err := record.Result()
		require.NoError(t, err.Get())
	}

	_, err = os.Stat(filepath.Join(dir, "wal_21000.log"))
	require.NoError(t, err)
}

func TestCompressedSegmentRoundTrip(t *testing.T) {
	directory := filepath.Join(testWALDirectory, "compressed_round_trip")
	require.NoError(t, os.MkdirAll(directory, os.ModePerm))

	logger := zerolog.Nop()
	compression := format.Compression{Codec: format.CodecZstd, MinFrameSize: 0}
	writer := NewFSWriter(directory, 100<<10, compression, &logger)

	batch := []Log{
		NewLog(1, compute.IncrCommandID, []string{"key_1", "60"}),
		NewLog(2, compute.IncrCommandID, []string{"key_2", "60"}),
	}
	writer.WriteBatch(batch)
	require.NoError(t, writer.Close())

	paths, err := walSegmentPaths(directory)
	require.NoError(t, err)
	require.Len(t, paths, 1)

	version, err := SegmentFormatVersion(paths[0])
	require.NoError(t, err)
	require.Equal(t, segmentFormatVersionCompressed, version)

	reader := NewFSReader(directory, &logger)
	logs, err := reader.ReadLogs(context.Background())
	require.NoError(t, err)
	require.Len(t, logs, 2)
	require.Equal(t, uint64(1), logs[0].LSN)
	require.Equal(t, uint64(2), logs[1].LSN)
}

func TestUncompressedWriterKeepsFormatVersionOne(t *testing.T) {
	directory := filepath.Join(testWALDirectory, "raw_version")
	require.NoError(t, os.MkdirAll(directory, os.ModePerm))

	logger := zerolog.Nop()
	writer := NewFSWriter(directory, 100<<10, format.Compression{}, &logger)

	writer.WriteBatch([]Log{NewLog(1, compute.IncrCommandID, []string{"key_1", "60"})})
	require.NoError(t, writer.Close())

	paths, err := walSegmentPaths(directory)
	require.NoError(t, err)
	require.Len(t, paths, 1)

	version, err := SegmentFormatVersion(paths[0])
	require.NoError(t, err)
	require.Equal(t, segmentFormatVersionRaw, version)
}

func TestReaderHandlesMixedSegmentVersions(t *testing.T) {
	directory := filepath.Join(testWALDirectory, "mixed_versions")
	require.NoError(t, os.MkdirAll(directory, os.ModePerm))

	logger := zerolog.Nop()

	rawWriter := NewFSWriter(directory, 100<<10, format.Compression{}, &logger)
	rawWriter.WriteBatch([]Log{NewLog(1, compute.IncrCommandID, []string{"key_1", "60"})})
	require.NoError(t, rawWriter.Close())

	compressedWriter := NewFSWriter(
		directory,
		100<<10,
		format.Compression{Codec: format.CodecS2, MinFrameSize: 0},
		&logger,
	)
	compressedWriter.WriteBatch([]Log{NewLog(2, compute.IncrCommandID, []string{"key_2", "60"})})
	require.NoError(t, compressedWriter.Close())

	reader := NewFSReader(directory, &logger)
	logs, err := reader.ReadLogs(context.Background())
	require.NoError(t, err)
	require.Len(t, logs, 2)
	require.Equal(t, uint64(1), logs[0].LSN)
	require.Equal(t, uint64(2), logs[1].LSN)
}

func TestCompressedSegmentTornTailIsTruncated(t *testing.T) {
	directory := filepath.Join(testWALDirectory, "torn_tail")
	require.NoError(t, os.MkdirAll(directory, os.ModePerm))

	logger := zerolog.Nop()
	compression := format.Compression{Codec: format.CodecS2, MinFrameSize: 0}
	writer := NewFSWriter(directory, 100<<10, compression, &logger)

	writer.WriteBatch([]Log{NewLog(1, compute.IncrCommandID, []string{"key_1", "60"})})
	writer.WriteBatch([]Log{NewLog(2, compute.IncrCommandID, []string{"key_2", "60"})})
	require.NoError(t, writer.Close())

	paths, err := walSegmentPaths(directory)
	require.NoError(t, err)
	require.Len(t, paths, 1)

	info, err := os.Stat(paths[0])
	require.NoError(t, err)
	require.NoError(t, os.Truncate(paths[0], info.Size()-2))

	reader := NewFSReader(directory, &logger)
	logs, err := reader.ReadLogs(context.Background())
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(1), logs[0].LSN)
}
