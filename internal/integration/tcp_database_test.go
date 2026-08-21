package integration

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"fq/internal/database"
	"fq/internal/database/compute"
	"fq/internal/database/storage"
	inmemory "fq/internal/database/storage/engine/in-memory"
	"fq/internal/database/storage/wal"
	"fq/internal/network"
)

func TestTCPDatabaseCommandsEndToEnd(t *testing.T) {
	app := startTestDatabase(t, t.TempDir())
	defer app.Close()

	app.RequireQuery("MSGSIZE", "ok|65536")
	app.RequireQuery("INCR key 60", "ok|1")
	app.RequireQuery("INCR key 60", "ok|2")
	app.RequireQuery("GET key 60", "ok|2")
	app.RequireQuery("INCR other 60", "ok|1")
	app.RequireQuery("MDEL key 60 other 60", "ok|1;1")
	app.RequireQuery("GET key 60", "ok|0")
	app.RequireQuery("TRUNCATE key 60", "err|invalid command")
}

func TestTCPDatabaseRecoversDataFromWALAfterRestart(t *testing.T) {
	walDir := t.TempDir()

	first := startTestDatabase(t, walDir)
	first.RequireQuery("INCR durable 60", "ok|1")
	first.RequireQuery("INCR durable 60", "ok|2")
	first.Close()

	second := startTestDatabase(t, walDir)
	defer second.Close()

	second.RequireQuery("GET durable 60", "ok|2")
}

func TestTCPDatabaseRecoversFromTruncatedWALTailAfterRestart(t *testing.T) {
	walDir := t.TempDir()

	first := startTestDatabase(t, walDir)
	first.RequireQuery("INCR durable 60", "ok|1")
	first.Close()

	segmentPath := lastWALSegmentPath(t, walDir)
	stat, err := os.Stat(segmentPath)
	require.NoError(t, err)
	validSize := stat.Size()

	appendTruncatedWALBatch(t, segmentPath)
	stat, err = os.Stat(segmentPath)
	require.NoError(t, err)
	require.Greater(t, stat.Size(), validSize)

	second := startTestDatabase(t, walDir)
	defer second.Close()

	second.RequireQuery("GET durable 60", "ok|1")

	stat, err = os.Stat(segmentPath)
	require.NoError(t, err)
	require.Equal(t, validSize, stat.Size())
}

type testDatabaseApp struct {
	t       *testing.T
	client  *network.TCPClient
	storage *storage.Storage
	cancel  context.CancelFunc
	done    chan error
}

func startTestDatabase(t *testing.T, walDir string) *testDatabaseApp {
	t.Helper()

	logger := zerolog.Nop()
	walStream := make(chan []*wal.LogData, 8)
	dumpStream := make(chan database.DumpChunk, 1)

	engine, err := inmemory.NewEngine(inmemory.HashTableBuilder, 4, &logger, walStream, dumpStream)
	require.NoError(t, err)

	walStore := newTestWAL(walDir, walStream, &logger)
	strg, err := storage.NewStorage(engine, walStore, nil, nil, &logger, time.Hour, time.Hour, true)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, strg.LoadWAL(ctx, database.NoTx))
	strg.Start(ctx)

	comp := compute.NewCompute(compute.NewParser(&logger), compute.NewAnalyzer(&logger), &logger)
	db := database.NewDatabase(comp, strg, &logger, 64<<10)
	address := freeLocalAddress(t)
	server, err := network.NewTCPServer(address, 8, 64<<10, time.Second, &logger)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- server.HandleQueries(ctx, func(ctx context.Context, query []byte) ([]byte, error) {
			return []byte(db.HandleQuery(ctx, string(query))), nil
		})
	}()

	client := connectEventually(t, address)

	return &testDatabaseApp{
		t:       t,
		client:  client,
		storage: strg,
		cancel:  cancel,
		done:    done,
	}
}

func newTestWAL(directory string, stream chan<- []*wal.LogData, logger *zerolog.Logger) *wal.WAL {
	return wal.NewWAL(
		wal.NewFSWriter(directory, 1<<20, logger),
		wal.NewFSReader(directory, logger),
		stream,
		time.Millisecond,
		16,
		directory,
		logger,
	)
}

func lastWALSegmentPath(t *testing.T, walDir string) string {
	t.Helper()

	segmentName, err := wal.SegmentLast(walDir)
	require.NoError(t, err)
	require.NotEmpty(t, segmentName)

	return filepath.Join(walDir, segmentName)
}

func appendTruncatedWALBatch(t *testing.T, segmentPath string) {
	t.Helper()

	file, err := os.OpenFile(segmentPath, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, file.Close())
	}()

	_, err = file.Write([]byte{0, 0, 0, 100, 1, 2})
	require.NoError(t, err)
	require.NoError(t, file.Sync())
}

func (a *testDatabaseApp) RequireQuery(query, expected string) {
	a.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	response, err := a.client.Send(ctx, []byte(query))
	require.NoError(a.t, err)
	require.Equal(a.t, expected, string(response))
}

func (a *testDatabaseApp) Close() {
	a.t.Helper()

	require.NoError(a.t, a.client.Close())
	a.cancel()
	a.storage.Shutdown()

	select {
	case err := <-a.done:
		require.NoError(a.t, err)
	case <-time.After(time.Second):
		a.t.Fatal("server did not stop")
	}
}

func freeLocalAddress(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, listener.Close())
	}()

	return listener.Addr().String()
}

func connectEventually(t *testing.T, address string) *network.TCPClient {
	t.Helper()

	var client *network.TCPClient
	require.Eventually(t, func() bool {
		var err error
		client, err = network.NewTCPClient(address, 64<<10, time.Second)

		return err == nil
	}, time.Second, 10*time.Millisecond)

	return client
}
