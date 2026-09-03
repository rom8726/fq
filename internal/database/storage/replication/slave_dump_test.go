package replication

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/database/storage/wal"
	"github.com/fq-db/fq/internal/security"
)

func TestSynchronizeDumpWaitsForEngineAckBeforeMarkingApplied(t *testing.T) {
	client := newStaticDumpClient(t, DumpResponse{
		Succeed:   true,
		EndOfDump: true,
		SegmentData: []database.DumpElem{
			{
				Key:       "key",
				BatchSize: 1,
				Value:     42,
				Tx:        7,
			},
		},
	})
	dumpStream := make(chan database.DumpChunk, 1)
	slave := newTestSlave(t, client, dumpStream)

	result := make(chan error, 1)
	go func() {
		result <- slave.synchronizeDump(context.Background())
	}()

	chunk := requireDumpChunk(t, dumpStream)
	require.Len(t, chunk.Elems, 1)
	require.NotNil(t, chunk.Applied)
	requireNotClosed(t, slave.dumpAppliedCh)
	requireNoResult(t, result)

	chunk.Applied <- nil

	require.NoError(t, requireErrorResult(t, result))
	require.False(t, slave.readDump)
	requireClosed(t, slave.dumpAppliedCh)
	require.Equal(t, uint64(7), slave.dumpLastSegmentNumber)
	require.Equal(t, uint64(7), slave.lastAppliedLSN)
}

func TestSynchronizeDumpPropagatesEngineApplyError(t *testing.T) {
	client := newStaticDumpClient(t, DumpResponse{
		Succeed:   true,
		EndOfDump: true,
		SegmentData: []database.DumpElem{
			{
				Key:       "key",
				BatchSize: 1,
				Value:     42,
				Tx:        7,
			},
		},
	})
	dumpStream := make(chan database.DumpChunk, 1)
	slave := newTestSlave(t, client, dumpStream)

	result := make(chan error, 1)
	go func() {
		result <- slave.synchronizeDump(context.Background())
	}()

	chunk := requireDumpChunk(t, dumpStream)
	applyErr := errors.New("apply failed")
	chunk.Applied <- applyErr

	require.ErrorIs(t, requireErrorResult(t, result), applyErr)
	require.True(t, slave.readDump)
	requireNotClosed(t, slave.dumpAppliedCh)
}

func TestSynchronizeDumpStopsWaitingForAckOnShutdown(t *testing.T) {
	client := newStaticDumpClient(t, DumpResponse{
		Succeed:   true,
		EndOfDump: true,
	})
	dumpStream := make(chan database.DumpChunk, 1)
	slave := newTestSlave(t, client, dumpStream)

	result := make(chan error, 1)
	go func() {
		result <- slave.synchronizeDump(context.Background())
	}()

	chunk := requireDumpChunk(t, dumpStream)
	require.NotNil(t, chunk.Applied)

	close(slave.closeCh)

	require.ErrorIs(t, requireErrorResult(t, result), errSlaveClosed)
	require.True(t, slave.readDump)
	requireNotClosed(t, slave.dumpAppliedCh)
}

type staticDumpClient struct {
	response []byte
}

func newStaticDumpClient(t *testing.T, response DumpResponse) *staticDumpClient {
	t.Helper()

	responseData, err := Encode(&response)
	require.NoError(t, err)

	return &staticDumpClient{response: responseData}
}

func (c *staticDumpClient) SendRaw(context.Context, []byte) ([]byte, error) {
	return c.response, nil
}

func (c *staticDumpClient) Close() error {
	return nil
}

type testWALReader struct{}

func (r testWALReader) ReadSegmentData(context.Context, []byte, bool, uint16) ([]*wal.LogData, error) {
	return nil, nil
}

func newTestSlave(t *testing.T, client TCPClient, dumpStream chan<- database.DumpChunk) *Slave {
	t.Helper()

	logger := zerolog.Nop()
	slave, err := NewSlave(
		client,
		"replica-test",
		security.Secret("replication-token-value"),
		":1946",
		testWALReader{},
		nil,
		dumpStream,
		t.TempDir(),
		time.Millisecond,
		&logger,
	)
	require.NoError(t, err)

	return slave
}

func requireDumpChunk(t *testing.T, dumpStream <-chan database.DumpChunk) database.DumpChunk {
	t.Helper()

	select {
	case chunk := <-dumpStream:
		return chunk
	case <-time.After(time.Second):
		t.Fatal("dump chunk was not sent")
	}

	return database.DumpChunk{}
}

func requireErrorResult(t *testing.T, result <-chan error) error {
	t.Helper()

	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("operation did not complete")
	}

	return nil
}

func requireNoResult(t *testing.T, result <-chan error) {
	t.Helper()

	select {
	case err := <-result:
		t.Fatalf("operation completed before ack: %v", err)
	default:
	}
}

func requireNotClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
		t.Fatal("channel was closed")
	default:
	}
}

func requireClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("channel was not closed")
	}
}
