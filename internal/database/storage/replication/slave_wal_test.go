package replication

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/database/storage/wal"
	"github.com/fq-db/fq/internal/security"
)

func TestSaveWALChunkAppendsAtOffset(t *testing.T) {
	directory := t.TempDir()
	slave := &Slave{walDirectory: directory}
	segmentName := "wal_1.log"
	segmentPath := filepath.Join(directory, segmentName)
	require.NoError(t, os.WriteFile(segmentPath, []byte("first"), 0o644))

	require.NoError(t, slave.saveWALChunk(segmentName, 5, []byte("second")))

	data, err := os.ReadFile(segmentPath)
	require.NoError(t, err)
	require.Equal(t, []byte("firstsecond"), data)
}

func TestSaveWALChunkTruncatesStaleTail(t *testing.T) {
	directory := t.TempDir()
	slave := &Slave{walDirectory: directory}
	segmentName := "wal_1.log"
	segmentPath := filepath.Join(directory, segmentName)
	require.NoError(t, os.WriteFile(segmentPath, []byte("first stale tail"), 0o644))

	require.NoError(t, slave.saveWALChunk(segmentName, 5, []byte("second")))

	data, err := os.ReadFile(segmentPath)
	require.NoError(t, err)
	require.Equal(t, []byte("firstsecond"), data)
}

func TestSaveWALChunkRejectsOffsetPastEnd(t *testing.T) {
	directory := t.TempDir()
	slave := &Slave{walDirectory: directory}
	segmentName := "wal_1.log"
	require.NoError(t, os.WriteFile(filepath.Join(directory, segmentName), []byte("first"), 0o644))

	require.Error(t, slave.saveWALChunk(segmentName, 6, []byte("second")))
}

func TestSlaveRewindsAckCursorIfLocalSegmentIsBehind(t *testing.T) {
	logger := zerolog.Nop()
	directory := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(directory, "wal_1.log"), nil, 0o644))
	slave := &Slave{
		walDirectory:      directory,
		lastSegmentName:   "wal_1.log",
		lastSegmentOffset: 1174,
		lastAppliedLSN:    7,
		closeCh:           make(chan struct{}),
		logger:            &logger,
	}

	err := slave.handleResponse(context.Background(), WALResponse{
		Succeed:           true,
		SegmentName:       "wal_1.log",
		SegmentOffset:     1174,
		NextSegmentOffset: 2048,
		SegmentData:       []byte("chunk"),
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "smaller than requested offset")
	require.Equal(t, "wal_1.log", slave.lastSegmentName)
	require.Equal(t, int64(0), slave.lastSegmentOffset)
	require.Equal(t, uint64(7), slave.lastAppliedLSN)
}

func TestSaveWALChunkRejectsUnsafeSegmentNames(t *testing.T) {
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
			require.Error(t, slave.saveWALChunk(segmentName, 0, []byte("data")))
		})
	}
}

func TestSlaveSendsLastAppliedLSNInNextWALRequest(t *testing.T) {
	logger := zerolog.Nop()
	walStream := make(chan wal.Chunk, 1)
	client := newRecordingWALClient(t, WALResponse{Succeed: true})
	walDirectory := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(walDirectory, "wal_1.log"), []byte("0123456789"), 0o644))
	slave := &Slave{
		client:            client,
		replicaID:         "replica-1",
		walReader:         scriptedWALReader{logs: []*wal.LogData{{LSN: 7}}},
		walStream:         walStream,
		walDirectory:      walDirectory,
		lastSegmentName:   "wal_1.log",
		lastSegmentOffset: 10,
		lastAppliedLSN:    3,
		closeCh:           make(chan struct{}),
		logger:            &logger,
	}

	result := make(chan error, 1)
	go func() {
		result <- slave.handleResponse(context.Background(), WALResponse{
			Succeed:           true,
			SegmentName:       "wal_1.log",
			SegmentOffset:     10,
			NextSegmentOffset: 20,
			SegmentData:       []byte("chunk"),
		})
	}()

	chunk := requireWALChunk(t, walStream)
	require.Len(t, chunk.Logs, 1)
	chunk.Applied <- nil
	require.NoError(t, requireErrorResult(t, result))

	require.NoError(t, slave.synchronizeWAL(context.Background()))

	require.Len(t, client.requests, 1)
	require.Equal(t, "replica-1", client.requests[0].ReplicaID)
	require.Equal(t, "wal_1.log", client.requests[0].LastSegmentName)
	require.Equal(t, int64(20), client.requests[0].SegmentOffset)
	require.Equal(t, uint64(7), client.requests[0].LastAppliedLSN)
}

func TestSlaveReconnectsAfterEOFAndAppliesWALChunk(t *testing.T) {
	logger := zerolog.Nop()
	walStream := make(chan wal.Chunk, 1)
	reconnectedClient := newRecordingWALClient(t, WALResponse{
		Succeed:           true,
		SegmentName:       "wal_1.log",
		SegmentOffset:     0,
		NextSegmentOffset: 10,
		SegmentData:       []byte("chunk"),
	})
	factory := &scriptedClientFactory{clients: []TCPClient{reconnectedClient}}
	initialClient := &failingWALClient{err: io.EOF}
	slave := &Slave{
		clientFactory:     factory,
		client:            initialClient,
		replicaID:         "replica-1",
		walReader:         scriptedWALReader{logs: []*wal.LogData{{LSN: 8}}},
		walStream:         walStream,
		walDirectory:      t.TempDir(),
		lastSegmentName:   "wal_1.log",
		lastSegmentOffset: 0,
		lastAppliedLSN:    3,
		closeCh:           make(chan struct{}),
		logger:            &logger,
	}

	result := make(chan error, 1)
	go func() {
		result <- slave.synchronizeWAL(context.Background())
	}()

	chunk := requireWALChunk(t, walStream)
	require.Len(t, chunk.Logs, 1)
	require.Equal(t, uint64(8), chunk.Logs[0].LSN)
	chunk.Applied <- nil

	require.NoError(t, requireErrorResult(t, result))
	require.True(t, initialClient.closed)
	require.Len(t, factory.clients, 0)
	require.Len(t, reconnectedClient.requests, 1)
	require.Equal(t, "replica-1", reconnectedClient.requests[0].ReplicaID)
	require.Equal(t, uint64(3), reconnectedClient.requests[0].LastAppliedLSN)
	require.Equal(t, uint64(8), slave.lastAppliedLSN)
	require.Equal(t, "wal_1.log", slave.lastSegmentName)
	require.Equal(t, int64(10), slave.lastSegmentOffset)
}

func TestNewSlaveInitializesCursorOffsetFromLastLocalSegment(t *testing.T) {
	logger := zerolog.Nop()
	directory := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(directory, "wal_1.log"), []byte("0123456789"), 0o644))
	client := newRecordingWALClient(t, WALResponse{Succeed: true})

	slave, err := NewSlave(
		client,
		"replica-1",
		security.Secret("replication-token-value"),
		":1946",
		scriptedWALReader{},
		make(chan wal.Chunk, 1),
		make(chan database.DumpChunk, 1),
		directory,
		time.Second,
		&logger,
	)

	require.NoError(t, err)
	require.Equal(t, "wal_1.log", slave.lastSegmentName)
	require.Equal(t, int64(10), slave.lastSegmentOffset)
}

func TestSlaveDoesNotUpdateAckCursorIfSaveWALChunkFails(t *testing.T) {
	logger := zerolog.Nop()
	directory := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(directory, "wal_1.log"), []byte("old"), 0o644))
	slave := &Slave{
		walReader:         scriptedWALReader{logs: []*wal.LogData{{LSN: 7}}},
		walStream:         make(chan wal.Chunk, 1),
		walDirectory:      directory,
		lastSegmentName:   "wal_1.log",
		lastSegmentOffset: 3,
		lastAppliedLSN:    5,
		closeCh:           make(chan struct{}),
		logger:            &logger,
	}

	err := slave.handleResponse(context.Background(), WALResponse{
		Succeed:           true,
		SegmentName:       "wal_1.log",
		SegmentOffset:     4,
		NextSegmentOffset: 10,
		SegmentData:       []byte("chunk"),
	})

	require.Error(t, err)
	require.Equal(t, "wal_1.log", slave.lastSegmentName)
	require.Equal(t, int64(3), slave.lastSegmentOffset)
	require.Equal(t, uint64(5), slave.lastAppliedLSN)
}

func TestSlaveDoesNotUpdateAckCursorIfApplyDataToEngineFails(t *testing.T) {
	logger := zerolog.Nop()
	walStream := make(chan wal.Chunk, 1)
	slave := &Slave{
		walReader:         scriptedWALReader{logs: []*wal.LogData{{LSN: 7}}},
		walStream:         walStream,
		walDirectory:      t.TempDir(),
		lastSegmentName:   "wal_1.log",
		lastSegmentOffset: 3,
		lastAppliedLSN:    5,
		closeCh:           make(chan struct{}),
		logger:            &logger,
	}

	result := make(chan error, 1)
	go func() {
		result <- slave.handleResponse(context.Background(), WALResponse{
			Succeed:           true,
			SegmentName:       "wal_1.log",
			SegmentOffset:     0,
			NextSegmentOffset: 10,
			SegmentData:       []byte("chunk"),
		})
	}()

	chunk := requireWALChunk(t, walStream)
	applyErr := errors.New("apply failed")
	chunk.Applied <- applyErr

	require.ErrorIs(t, requireErrorResult(t, result), applyErr)
	require.Equal(t, "wal_1.log", slave.lastSegmentName)
	require.Equal(t, int64(3), slave.lastSegmentOffset)
	require.Equal(t, uint64(5), slave.lastAppliedLSN)
}

type recordingWALClient struct {
	t        *testing.T
	response []byte
	requests []WALRequest
}

func newRecordingWALClient(t *testing.T, response WALResponse) *recordingWALClient {
	t.Helper()

	responseData, err := Encode(&response)
	require.NoError(t, err)

	return &recordingWALClient{
		t:        t,
		response: responseData,
	}
}

func (c *recordingWALClient) Send(_ context.Context, data []byte) ([]byte, error) {
	var request Request
	require.NoError(c.t, Decode(&request, data))
	c.requests = append(c.requests, request.WALRequest)

	return c.response, nil
}

func (c *recordingWALClient) Close() error {
	return nil
}

type failingWALClient struct {
	err    error
	closed bool
}

func (c *failingWALClient) Send(context.Context, []byte) ([]byte, error) {
	return nil, c.err
}

func (c *failingWALClient) Close() error {
	c.closed = true

	return nil
}

type scriptedClientFactory struct {
	clients []TCPClient
}

func (f *scriptedClientFactory) Create() (TCPClient, error) {
	if len(f.clients) == 0 {
		return nil, errors.New("no scripted clients left")
	}

	client := f.clients[0]
	f.clients = f.clients[1:]

	return client, nil
}

type scriptedWALReader struct {
	logs []*wal.LogData
	err  error
}

func (r scriptedWALReader) ReadSegmentData(context.Context, []byte) ([]*wal.LogData, error) {
	return r.logs, r.err
}

func requireWALChunk(t *testing.T, walStream <-chan wal.Chunk) wal.Chunk {
	t.Helper()

	select {
	case chunk := <-walStream:
		return chunk
	case <-time.After(time.Second):
		t.Fatal("WAL chunk was not sent")
	}

	return wal.Chunk{}
}
