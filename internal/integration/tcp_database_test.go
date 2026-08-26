package integration

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/fq-db/fq/internal/config"
	"github.com/fq-db/fq/internal/database"
	"github.com/fq-db/fq/internal/database/compute"
	"github.com/fq-db/fq/internal/database/storage"
	"github.com/fq-db/fq/internal/database/storage/dumper"
	inmemory "github.com/fq-db/fq/internal/database/storage/engine/in-memory"
	"github.com/fq-db/fq/internal/database/storage/wal"
	"github.com/fq-db/fq/internal/network"
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

func TestTCPDatabaseRejectsInvalidInputsWithoutMutatingState(t *testing.T) {
	app := startTestDatabase(t, t.TempDir())
	defer app.Close()

	app.RequireQuery("INCR stable 600", "ok|1")

	oversizedKey := strings.Repeat("k", 1025)
	tests := []string{
		"INCR stable not-a-window",
		"INCR stable 0",
		"GET stable not-a-window",
		"DEL stable not-a-window",
		"MDEL stable 600 other",
		"RLIMIT XX stable 2 600",
		"RLIMIT FW stable bad-limit 600",
		"RLIMIT FW stable 0 600",
		"RLIMIT TB stable 10 bad-refill 600",
		"RLIMIT TB stable 10 0 600",
		"INCR " + oversizedKey + " 600",
	}

	for _, query := range tests {
		t.Run(query, func(t *testing.T) {
			response := app.RequireQueryPrefix(query, "err|")
			require.NotEmpty(t, strings.TrimPrefix(response, "err|"))
			app.RequireQuery("GET stable 600", "ok|1")
		})
	}
}

func TestTCPDatabaseMatchesReferenceModelForDeterministicSequence(t *testing.T) {
	app := startTestDatabase(t, t.TempDir())
	defer app.Close()

	model := newReferenceModel()
	queries := []string{
		"GET user_a 600",
		"INCR user_a 600",
		"INCR user_a 600",
		"GET user_a 600",
		"INCR user_b 60",
		"MDEL user_a 600 user_b 60 missing 600",
		"GET user_a 600",
		"RLIMIT FW fw_user 2 600",
		"RLIMIT FW fw_user 2 600",
		"RLIMIT FW fw_user 2 600",
		"GET fw_user 600",
		"DEL fw_user 600",
		"RLIMIT FW fw_user 2 600",
		"RLIMIT SW sw_user 2 600",
		"RLIMIT SW sw_user 2 600",
		"RLIMIT SW sw_user 2 600",
		"DEL sw_user 600",
		"RLIMIT SW sw_user 2 600",
		"RLIMIT TB tb_user 3 1 600",
		"RLIMIT TB tb_user 3 1 600",
		"RLIMIT TB tb_user 3 1 600",
		"RLIMIT TB tb_user 3 1 600",
		"DEL tb_user 600",
		"RLIMIT TB tb_user 3 1 600",
	}

	for _, query := range queries {
		expected := model.Apply(t, query)
		actual := app.RequireOK(query)
		requireModelResponse(t, query, expected, actual)
	}
}

func TestTCPDatabaseIncrHotKeyConcurrently(t *testing.T) {
	app := startTestDatabase(t, t.TempDir())
	defer app.Close()

	const workers = 32
	const incrementsPerWorker = 50

	var successful atomic.Int32
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			client := connectEventually(t, app.address)
			defer func() {
				if err := client.Close(); err != nil {
					errs <- err
				}
			}()

			for i := 0; i < incrementsPerWorker; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				response, err := client.Send(ctx, []byte("INCR hot 600"))
				cancel()
				if err != nil {
					errs <- err

					return
				}
				if !strings.HasPrefix(string(response), "ok|") {
					errs <- fmt.Errorf("unexpected response: %s", response)

					return
				}
				successful.Add(1)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	app.RequireQuery("GET hot 600", fmt.Sprintf("ok|%d", successful.Load()))
}

func TestTCPDatabaseRLimitDoesNotExceedLimitConcurrently(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		checkGet bool
	}{
		{name: "FW", query: "RLIMIT FW limited 10 600", checkGet: true},
		{name: "SW", query: "RLIMIT SW limited 10 600"},
		{name: "TB", query: "RLIMIT TB limited 10 1 600"},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			app := startTestDatabase(t, t.TempDir())
			defer app.Close()

			const limit = 10
			const workers = 64

			var allowed atomic.Int32
			errs := make(chan error, workers)
			var wg sync.WaitGroup
			for worker := 0; worker < workers; worker++ {
				wg.Add(1)
				go func() {
					defer wg.Done()

					client := connectEventually(t, app.address)
					defer func() {
						if err := client.Close(); err != nil {
							errs <- err
						}
					}()

					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					response, err := client.Send(ctx, []byte(test.query))
					cancel()
					if err != nil {
						errs <- err

						return
					}
					result, err := parseRateLimitResponse(string(response))
					if err != nil {
						errs <- err

						return
					}
					if result.allowed {
						allowed.Add(1)
					}
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				require.NoError(t, err)
			}

			require.Equal(t, int32(limit), allowed.Load())
			if test.checkGet {
				app.RequireQuery("GET limited 600", "ok|10")
			}
		})
	}
}

func TestTCPDatabaseDumpDuringWriteLoadRecoversAllAcknowledgedWrites(t *testing.T) {
	walDir := t.TempDir()
	dumpDir := t.TempDir()

	first := startTestDatabaseWithDump(t, walDir, dumpDir, false)

	const totalWrites = 200
	var successful atomic.Int32
	dumpErr := make(chan error, 1)
	for i := 0; i < totalWrites; i++ {
		first.RequireQuery("INCR durable_hot 600", fmt.Sprintf("ok|%d", i+1))
		successful.Add(1)
		if i == totalWrites/2 {
			go func(tx database.Tx) {
				dumpErr <- first.dumper.Dump(context.Background(), tx)
			}(database.Tx(successful.Load()))
		}
	}
	select {
	case err := <-dumpErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("dump during write load did not finish")
	}
	require.NoError(t, first.dumper.Dump(context.Background(), database.Tx(successful.Load())))
	first.Close()

	second := startTestDatabaseWithDump(t, walDir, dumpDir, true)
	defer second.Close()

	second.RequireQuery("GET durable_hot 600", fmt.Sprintf("ok|%d", successful.Load()))
}

func TestTCPDatabasePStreamFiltersLimitEventsByPrefix(t *testing.T) {
	app := startTestDatabase(t, t.TempDir())
	defer app.Close()

	streamClient := connectEventually(t, app.address)

	events := make(chan string, 2)
	errs := make(chan error, 1)
	go func() {
		errs <- streamClient.Stream(context.Background(), []byte("PSTREAM tenant_a-"), func(response []byte) error {
			events <- string(response)

			return nil
		})
	}()

	app.RequireRateLimit("RLIMIT FW tenant_b-user_42 1 60", true, 1, 0, 60)
	requireNoStreamEvent(t, events)

	app.RequireRateLimit("RLIMIT FW tenant_a-user_42 1 60", true, 1, 0, 60)
	event := requireStreamEvent(t, events)
	require.True(t, strings.HasPrefix(event, "ok|tenant_a-user_42;60;1;"))

	require.NoError(t, streamClient.Close())
	select {
	case <-errs:
	case <-time.After(time.Second):
		t.Fatal("stream did not stop")
	}
}

func requireNoStreamEvent(t *testing.T, events <-chan string) {
	t.Helper()

	select {
	case event := <-events:
		t.Fatalf("unexpected stream event: %s", event)
	case <-time.After(100 * time.Millisecond):
	}
}

func requireStreamEvent(t *testing.T, events <-chan string) string {
	t.Helper()

	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for stream event")
	}

	return ""
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
	address string
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
	server, err := network.NewTCPServer(address, 128, 64<<10, time.Second, &logger)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- server.HandleQueryStreams(ctx, func(
			ctx context.Context,
			query []byte,
			write func([]byte) error,
		) error {
			return db.HandleQueryStream(ctx, string(query), func(response string) error {
				return write([]byte(response))
			})
		})
	}()

	client := connectEventually(t, address)

	return &testDatabaseApp{
		t:       t,
		address: address,
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

func (a *testDatabaseApp) RequireQueryPrefix(query, prefix string) string {
	a.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	response, err := a.client.Send(ctx, []byte(query))
	require.NoError(a.t, err)
	require.True(a.t, strings.HasPrefix(string(response), prefix), string(response))

	return string(response)
}

func (a *testDatabaseApp) RequireOK(query string) string {
	a.t.Helper()

	return a.RequireQueryPrefix(query, "ok|")
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

type referenceModel struct {
	counters map[referenceKey]int
	buckets  map[referenceKey]int
}

type referenceKey struct {
	key    string
	window string
}

type expectedResponse struct {
	raw       string
	rateLimit *expectedRateLimit
}

type expectedRateLimit struct {
	allowed   bool
	current   int
	remaining int
	window    uint32
}

type actualRateLimit struct {
	allowed    bool
	current    int
	remaining  int
	resetAfter uint32
}

func newReferenceModel() *referenceModel {
	return &referenceModel{
		counters: make(map[referenceKey]int),
		buckets:  make(map[referenceKey]int),
	}
}

func (m *referenceModel) Apply(t *testing.T, query string) expectedResponse {
	t.Helper()

	parts := strings.Fields(query)
	require.NotEmpty(t, parts)

	switch parts[0] {
	case "INCR":
		key := referenceKey{key: parts[1], window: parts[2]}
		m.counters[key]++

		return expectedResponse{raw: fmt.Sprintf("ok|%d", m.counters[key])}
	case "GET":
		key := referenceKey{key: parts[1], window: parts[2]}

		return expectedResponse{raw: fmt.Sprintf("ok|%d", m.counters[key])}
	case "DEL":
		key := referenceKey{key: parts[1], window: parts[2]}
		_, counterFound := m.counters[key]
		_, bucketFound := m.buckets[key]
		delete(m.counters, key)
		delete(m.buckets, key)

		if counterFound || bucketFound {
			return expectedResponse{raw: "ok|1"}
		}

		return expectedResponse{raw: "ok|0"}
	case "MDEL":
		results := make([]string, 0, len(parts)/2)
		for i := 1; i < len(parts); i += 2 {
			key := referenceKey{key: parts[i], window: parts[i+1]}
			_, counterFound := m.counters[key]
			_, bucketFound := m.buckets[key]
			delete(m.counters, key)
			delete(m.buckets, key)
			if counterFound || bucketFound {
				results = append(results, "1")
			} else {
				results = append(results, "0")
			}
		}

		return expectedResponse{raw: "ok|" + strings.Join(results, ";")}
	case "RLIMIT":
		algorithm := parts[1]
		window := parts[4]
		if algorithm == "TB" {
			window = parts[5]
		}
		parsedWindow, err := strconv.ParseUint(window, 10, 32)
		require.NoError(t, err)
		limit, err := strconv.Atoi(parts[3])
		require.NoError(t, err)
		key := referenceKey{key: parts[2], window: window}

		if algorithm == "TB" {
			used := m.buckets[key]
			if used >= limit {
				return expectedResponse{rateLimit: &expectedRateLimit{
					allowed:   false,
					current:   used,
					remaining: 0,
					window:    uint32(parsedWindow),
				}}
			}

			used++
			m.buckets[key] = used

			return expectedResponse{rateLimit: &expectedRateLimit{
				allowed:   true,
				current:   used,
				remaining: limit - used,
				window:    uint32(parsedWindow),
			}}
		}

		current := m.counters[key]
		if current >= limit {
			return expectedResponse{rateLimit: &expectedRateLimit{
				allowed:   false,
				current:   current,
				remaining: 0,
				window:    uint32(parsedWindow),
			}}
		}

		current++
		m.counters[key] = current

		return expectedResponse{rateLimit: &expectedRateLimit{
			allowed:   true,
			current:   current,
			remaining: limit - current,
			window:    uint32(parsedWindow),
		}}
	default:
		t.Fatalf("unsupported reference model query: %s", query)
	}

	return expectedResponse{}
}

func requireModelResponse(t *testing.T, query string, expected expectedResponse, actual string) {
	t.Helper()

	if expected.rateLimit == nil {
		require.Equal(t, expected.raw, actual)

		return
	}

	result := requireRateLimitResponse(t, actual)
	require.Equal(t, expected.rateLimit.allowed, result.allowed, query)
	require.Equal(t, expected.rateLimit.current, result.current, query)
	require.Equal(t, expected.rateLimit.remaining, result.remaining, query)
	require.LessOrEqual(t, result.resetAfter, expected.rateLimit.window, query)
}

func requireRateLimitResponse(t *testing.T, response string) actualRateLimit {
	t.Helper()

	result, err := parseRateLimitResponse(response)
	require.NoError(t, err)

	return result
}

func parseRateLimitResponse(response string) (actualRateLimit, error) {
	parts := strings.Split(response, "|")
	if len(parts) != 2 {
		return actualRateLimit{}, fmt.Errorf("expected two response parts, got %q", response)
	}
	if parts[0] != "ok" {
		return actualRateLimit{}, fmt.Errorf("expected ok response, got %q", response)
	}

	fields := strings.Split(parts[1], ";")
	if len(fields) != 4 {
		return actualRateLimit{}, fmt.Errorf("expected four rate-limit fields, got %q", response)
	}
	allowed, err := strconv.Atoi(fields[0])
	if err != nil {
		return actualRateLimit{}, err
	}
	current, err := strconv.Atoi(fields[1])
	if err != nil {
		return actualRateLimit{}, err
	}
	remaining, err := strconv.Atoi(fields[2])
	if err != nil {
		return actualRateLimit{}, err
	}
	resetAfter, err := strconv.ParseUint(fields[3], 10, 32)
	if err != nil {
		return actualRateLimit{}, err
	}

	return actualRateLimit{
		allowed:    allowed == 1,
		current:    current,
		remaining:  remaining,
		resetAfter: uint32(resetAfter),
	}, nil
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
