package integration

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"fq/internal/config"
	"fq/internal/database"
	"fq/internal/database/compute"
	"fq/internal/database/storage"
	"fq/internal/database/storage/dumper"
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
	app.RequireRateLimit("RLIMIT FW limited 2 60", true, 1, 1, 60)
	app.RequireRateLimit("RLIMIT FW limited 2 60", true, 2, 0, 60)
	app.RequireRateLimit("RLIMIT FW limited 2 60", false, 2, 0, 60)
	app.RequireQuery("GET limited 60", "ok|2")
	app.RequireRateLimit("RLIMIT SW sliding 2 60", true, 1, 1, 60)
	app.RequireRateLimit("RLIMIT SW sliding 2 60", true, 2, 0, 60)
	app.RequireRateLimit("RLIMIT SW sliding 2 60", false, 2, 0, 60)
	app.RequireQuery("DEL sliding 60", "ok|1")
	app.RequireRateLimit("RLIMIT SW sliding 2 60", true, 1, 1, 60)
	app.RequireRateLimit("RLIMIT TB bucket 3 1 60", true, 1, 2, 60)
	app.RequireRateLimit("RLIMIT TB bucket 3 1 60", true, 2, 1, 60)
	app.RequireRateLimit("RLIMIT TB bucket 3 1 60", true, 3, 0, 60)
	app.RequireRateLimit("RLIMIT TB bucket 3 1 60", false, 3, 0, 60)
	app.RequireQuery("DEL bucket 60", "ok|1")
	app.RequireRateLimit("RLIMIT TB bucket 3 1 60", true, 1, 2, 60)
	app.RequireQuery("MDEL key 60 other 60", "ok|1;1")
	app.RequireQuery("GET key 60", "ok|0")
	app.RequireQuery("TRUNCATE key 60", "err|invalid command")
	app.RequireQuery("RLIMIT XX limited 2 60", "err|invalid rate limit algorithm")
}

func TestTCPDatabaseRecoversDataFromWALAfterRestart(t *testing.T) {
	walDir := t.TempDir()

	first := startTestDatabase(t, walDir)
	first.RequireQuery("INCR durable 60", "ok|1")
	first.RequireQuery("INCR durable 60", "ok|2")
	first.RequireRateLimit("RLIMIT FW limited 2 60", true, 1, 1, 60)
	first.RequireRateLimit("RLIMIT FW limited 2 60", true, 2, 0, 60)
	first.RequireRateLimit("RLIMIT FW limited 2 60", false, 2, 0, 60)
	first.RequireRateLimit("RLIMIT SW sliding 2 60", true, 1, 1, 60)
	first.RequireRateLimit("RLIMIT SW sliding 2 60", true, 2, 0, 60)
	first.RequireRateLimit("RLIMIT SW sliding 2 60", false, 2, 0, 60)
	first.RequireRateLimit("RLIMIT TB bucket 3 1 600", true, 1, 2, 600)
	first.RequireRateLimit("RLIMIT TB bucket 3 1 600", true, 2, 1, 600)
	first.Close()

	second := startTestDatabase(t, walDir)
	defer second.Close()

	second.RequireQuery("GET durable 60", "ok|2")
	second.RequireQuery("GET limited 60", "ok|2")
	second.RequireRateLimit("RLIMIT SW sliding 2 60", false, 2, 0, 60)
	second.RequireRateLimit("RLIMIT TB bucket 3 1 600", true, 3, 0, 600)
}

func TestTCPDatabaseRecoversSlidingWindowFromDumpAfterRestart(t *testing.T) {
	walDir := t.TempDir()
	dumpDir := t.TempDir()

	first := startTestDatabaseWithDump(t, walDir, dumpDir, false)
	for i := 1; i <= 6; i++ {
		first.RequireRateLimit("RLIMIT SW key_sw 10 600", true, database.ValueType(i), database.ValueType(10-i), 600)
	}
	require.NoError(t, first.dumper.Dump(context.Background(), database.Tx(6)))
	first.Close()

	second := startTestDatabaseWithDump(t, walDir, dumpDir, true)
	defer second.Close()

	second.RequireRateLimit("RLIMIT SW key_sw 10 600", true, 7, 3, 600)
}

func TestTCPDatabaseRecoversTokenBucketFromDumpAfterRestart(t *testing.T) {
	walDir := t.TempDir()
	dumpDir := t.TempDir()

	first := startTestDatabaseWithDump(t, walDir, dumpDir, false)
	first.RequireRateLimit("RLIMIT TB key_tb 5 1 600", true, 1, 4, 600)
	first.RequireRateLimit("RLIMIT TB key_tb 5 1 600", true, 2, 3, 600)
	first.RequireRateLimit("RLIMIT TB key_tb 5 1 600", true, 3, 2, 600)
	require.NoError(t, first.dumper.Dump(context.Background(), database.Tx(3)))
	first.Close()

	second := startTestDatabaseWithDump(t, walDir, dumpDir, true)
	defer second.Close()

	second.RequireRateLimit("RLIMIT TB key_tb 5 1 600", true, 4, 1, 600)
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
	dumper  *dumper.Dumper
	cancel  context.CancelFunc
	done    chan error
}

func startTestDatabase(t *testing.T, walDir string) *testDatabaseApp {
	return startTestDatabaseWithDump(t, walDir, "", false)
}

func startTestDatabaseWithDump(t *testing.T, walDir, dumpDir string, restoreDump bool) *testDatabaseApp {
	t.Helper()

	logger := zerolog.Nop()
	walStream := make(chan wal.Chunk, 8)
	dumpStream := make(chan database.DumpChunk, 1)

	engine, err := inmemory.NewEngine(inmemory.HashTableBuilder, 4, &logger, walStream, dumpStream)
	require.NoError(t, err)

	walStore := newTestWAL(walDir, walStream, &logger)
	var dumpStore *dumper.Dumper
	var dumpStorage storage.Dumper
	if dumpDir != "" {
		dumpStore = dumper.New(engine, walStore, dumpDir)
		dumpStorage = dumpStore
	}
	strg, err := storage.NewStorage(
		engine,
		walStore,
		dumpStorage,
		nil,
		&logger,
		time.Hour,
		time.Hour,
		true,
		config.DefaultLimitEventQueueCapacity,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	lastTx := database.NoTx
	if restoreDump && dumpStore != nil {
		lastTx, err = dumpStore.Restore(ctx)
		require.NoError(t, err)
	}
	require.NoError(t, strg.LoadWAL(ctx, lastTx))
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
		dumper:  dumpStore,
		cancel:  cancel,
		done:    done,
	}
}

func newTestWAL(directory string, stream chan<- wal.Chunk, logger *zerolog.Logger) *wal.WAL {
	return wal.NewWAL(
		wal.NewFSWriter(directory, 1<<20, logger),
		wal.NewFSReader(directory, logger),
		stream,
		time.Millisecond,
		16,
		64,
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

func (a *testDatabaseApp) RequireRateLimit(
	query string,
	allowed bool,
	current database.ValueType,
	remaining database.ValueType,
	window uint32,
) {
	a.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	response, err := a.client.Send(ctx, []byte(query))
	require.NoError(a.t, err)

	parts := strings.Split(string(response), "|")
	require.Len(a.t, parts, 2)
	require.Equal(a.t, "ok", parts[0])

	fields := strings.Split(parts[1], ";")
	require.Len(a.t, fields, 4)
	if allowed {
		require.Equal(a.t, "1", fields[0])
	} else {
		require.Equal(a.t, "0", fields[0])
	}
	require.Equal(a.t, strconv.FormatInt(int64(current), 10), fields[1])
	require.Equal(a.t, strconv.FormatInt(int64(remaining), 10), fields[2])

	resetAfter, err := strconv.ParseUint(fields[3], 10, 32)
	require.NoError(a.t, err)
	require.GreaterOrEqual(a.t, uint32(resetAfter), uint32(0))
	require.LessOrEqual(a.t, uint32(resetAfter), window)
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
