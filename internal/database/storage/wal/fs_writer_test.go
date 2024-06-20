package wal

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"fq/internal/database/compute"
)

const testWALDirectory = "/tmp/fq_wal_test_data"

func TestMain(m *testing.M) {
	if err := os.Mkdir(testWALDirectory, os.ModePerm); err != nil {
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
	fsWriter := NewFSWriter(testWALDirectory, maxSegmentSize, &logger)

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
}

func TestWALSegmentsRotation(t *testing.T) {
	maxSegmentSize := 10
	logger := zerolog.Nop()
	fsWriter := NewFSWriter(testWALDirectory, maxSegmentSize, &logger)

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

	stat, err = os.Stat(testWALDirectory + "/wal_3000.log")
	require.NoError(t, err)
	require.NotZero(t, stat.Size())
}
