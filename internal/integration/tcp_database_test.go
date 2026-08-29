package integration

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
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
	"github.com/fq-db/fq/internal/database/storage/replication"
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
	app.RequireQuotaAcquire("QUOTA ACQ quota 10 4 client-a 60", true, 4, 4, 6, 60)
	app.RequireQuotaAcquire("QUOTA ACQ quota 10 4 client-a 60", true, 4, 4, 6, 60)
	app.RequireQuotaInfo("QUOTA INF quota", 10, 4, 6, []testQuotaClient{
		{clientID: "client-a", amount: 4, expires: true},
	})
	app.RequireQuotaAcquire("QUOTA ACQ quota 10 7 client-b", false, 0, 4, 6, 0)
	app.RequireQuery("QUOTA DEL quota", "err|quota is not empty")
	app.RequireQuery("QUOTA REL quota client-a", "ok|1")
	app.RequireQuery("QUOTA DEL quota", "ok|1")
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
		"",
		"   \t  \n  ",
		"INCR stable not-a-window",
		"INCR stable 0",
		"INCR stable -1",
		"INCR stable 4294967296",
		"GET stable not-a-window",
		"DEL stable not-a-window",
		"MDEL stable 600 other",
		"RLIMIT XX stable 2 600",
		"RLIMIT FW stable bad-limit 600",
		"RLIMIT FW stable 0 600",
		"RLIMIT FW stable -1 600",
		"RLIMIT FW stable 2147483648 600",
		"RLIMIT FW stable 2 4294967296",
		"RLIMIT TB stable 10 bad-refill 600",
		"RLIMIT TB stable 10 0 600",
		"RLIMIT TB stable 2147483648 1 600",
		"RLIMIT TB stable 10 2147483648 600",
		"RLIMIT TB stable 10 1 4294967296",
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

func TestTCPDatabaseAcceptsBoundaryInputs(t *testing.T) {
	app := startTestDatabase(t, t.TempDir())
	defer app.Close()

	maxKey := strings.Repeat("k", 1024)
	app.RequireQuery("INCR "+maxKey+" 600", "ok|1")
	app.RequireQuery("GET "+maxKey+" 600", "ok|1")

	app.RequireQuery("INCR max_window 4294967295", "ok|1")
	app.RequireQuery("GET max_window 4294967295", "ok|1")

	app.RequireRateLimit("RLIMIT FW max_limit 2147483647 600", true, 1, 2147483646, 600)
}

func TestTCPDatabaseCommandsAreCaseInsensitive(t *testing.T) {
	app := startTestDatabase(t, t.TempDir())
	defer app.Close()

	app.RequireQuery("incr mixed_case 600", "ok|1")
	app.RequireQuery("get mixed_case 600", "ok|1")
	app.RequireRateLimit("rlimit fw lower_fw 2 600", true, 1, 1, 600)
	app.RequireRateLimit("rlimit sw lower_sw 2 600", true, 1, 1, 600)
	app.RequireRateLimit("rlimit tb lower_tb 2 1 600", true, 1, 1, 600)
	app.RequireQuery("del mixed_case 600", "ok|1")
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

func TestTCPDatabaseMatchesReferenceModelForSeededSequence(t *testing.T) {
	const seed = int64(2026082601)
	app := startTestDatabase(t, t.TempDir())
	defer app.Close()

	model := newReferenceModel()
	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < 500; i++ {
		query := randomModelQuery(rng)
		expected := model.Apply(t, query)
		actual := app.RequireOK(query)
		requireModelResponse(t, fmt.Sprintf("seed=%d op=%d query=%s", seed, i, query), expected, actual)
	}
}

func TestTCPDatabaseRecoversSeededModelSequenceAcrossRestarts(t *testing.T) {
	const seed = int64(2026082602)
	const operations = 240
	const restartEvery = 40

	walDir := t.TempDir()
	app := startTestDatabase(t, walDir)
	defer func() {
		app.Close()
	}()

	model := newReferenceModel()
	rng := rand.New(rand.NewSource(seed))
	for i := 0; i < operations; i++ {
		query := randomModelQuery(rng)
		expected := model.Apply(t, query)
		actual := app.RequireOK(query)
		requireModelResponse(t, fmt.Sprintf("seed=%d op=%d query=%s", seed, i, query), expected, actual)

		if (i+1)%restartEvery == 0 {
			checkpointQuery := fmt.Sprintf("INCR recovery_checkpoint_%03d 1000000000", i)
			expected = model.Apply(t, checkpointQuery)
			actual = app.RequireOK(checkpointQuery)
			requireModelResponse(t, fmt.Sprintf("seed=%d checkpoint_after_op=%d query=%s", seed, i, checkpointQuery), expected, actual)

			app.Close()
			app = startTestDatabase(t, walDir)
			requireReferenceModelSamples(t, app, model, fmt.Sprintf("seed=%d after_op=%d", seed, i))
		}
	}
}

func TestTCPDatabaseWatchWakesOnIncr(t *testing.T) {
	app := startTestDatabase(t, t.TempDir())
	defer app.Close()

	watchClient := connectEventuallyWithIdle(t, app.address, 3*time.Second)
	watch := sendQueryAsync(watchClient, "WATCH watched 600", 2*time.Second)

	time.Sleep(150 * time.Millisecond)
	app.RequireQuery("INCR watched 600", "ok|1")

	requireAsyncResponse(t, watch, "ok|1")
}

func TestTCPDatabaseWatchWakesOnFixedWindowRateLimit(t *testing.T) {
	app := startTestDatabase(t, t.TempDir())
	defer app.Close()

	watchClient := connectEventuallyWithIdle(t, app.address, 3*time.Second)
	watch := sendQueryAsync(watchClient, "WATCH limited_watch 600", 2*time.Second)

	time.Sleep(150 * time.Millisecond)
	app.RequireRateLimit("RLIMIT FW limited_watch 3 600", true, 1, 2, 600)

	requireAsyncResponse(t, watch, "ok|1")
}

func TestTCPDatabaseWatchTimesOutWithoutChange(t *testing.T) {
	app := startTestDatabase(t, t.TempDir())
	defer app.Close()

	watchClient := connectEventuallyWithIdle(t, app.address, 3*time.Second)
	watch := sendQueryAsync(watchClient, "WATCH unchanged 600", 250*time.Millisecond)

	requireAsyncTimeout(t, watch)
}

func TestTCPDatabaseMultipleWatchersWakeOnOneChange(t *testing.T) {
	app := startTestDatabase(t, t.TempDir())
	defer app.Close()

	const watchers = 12
	results := make([]<-chan asyncQueryResult, 0, watchers)
	for i := 0; i < watchers; i++ {
		client := connectEventuallyWithIdle(t, app.address, 3*time.Second)
		results = append(results, sendQueryAsync(client, "WATCH watched_by_many 600", 2*time.Second))
	}

	time.Sleep(150 * time.Millisecond)
	app.RequireQuery("INCR watched_by_many 600", "ok|1")

	for _, result := range results {
		requireAsyncResponse(t, result, "ok|1")
	}
}

func TestTCPDatabaseWatchersDoNotBlockUnrelatedWriters(t *testing.T) {
	app := startTestDatabase(t, t.TempDir())
	defer app.Close()

	const watchers = 16
	results := make([]<-chan asyncQueryResult, 0, watchers)
	for i := 0; i < watchers; i++ {
		client := connectEventuallyWithIdle(t, app.address, 3*time.Second)
		results = append(results, sendQueryAsync(client, fmt.Sprintf("WATCH idle_%02d 600", i), 300*time.Millisecond))
	}

	time.Sleep(150 * time.Millisecond)
	app.RequireQuery("INCR unrelated_writer 600", "ok|1")

	for _, result := range results {
		requireAsyncTimeout(t, result)
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

func TestTCPDatabaseSlaveEventuallyConvergesWithMaster(t *testing.T) {
	masterWALDir := t.TempDir()
	masterDumpDir := t.TempDir()
	slaveWALDir := t.TempDir()
	replicationAddress := freeLocalAddress(t)

	masterApp := startTestDatabaseWithMasterReplication(t, masterWALDir, masterDumpDir, replicationAddress)
	defer masterApp.Close()
	waitTCPAddress(t, replicationAddress, 16<<20)

	masterApp.RequireQuery("INCR replicated_counter 600", "ok|1")
	masterApp.RequireQuery("INCR replicated_counter 600", "ok|2")
	masterApp.RequireRateLimit("RLIMIT FW replicated_fw 2 600", true, 1, 1, 600)
	masterApp.RequireRateLimit("RLIMIT FW replicated_fw 2 600", true, 2, 0, 600)
	masterApp.RequireRateLimit("RLIMIT SW replicated_sw 2 600", true, 1, 1, 600)
	masterApp.RequireRateLimit("RLIMIT SW replicated_sw 2 600", true, 2, 0, 600)
	masterApp.RequireRateLimit("RLIMIT TB replicated_tb 3 1 600", true, 1, 2, 600)
	masterApp.RequireRateLimit("RLIMIT TB replicated_tb 3 1 600", true, 2, 1, 600)

	slave := startTestDatabaseWithSlaveReplication(t, slaveWALDir, replicationAddress)
	slaveClosed := false
	defer func() {
		if !slaveClosed {
			slave.Close()
		}
	}()

	require.Eventually(t, func() bool {
		return masterApp.MinReplicaAckLSN() >= 8
	}, 5*time.Second, 25*time.Millisecond)

	slave.RequireQuery("GET replicated_counter 600", "ok|2")
	slave.RequireQuery("GET replicated_fw 600", "ok|2")
	slave.RequireRateLimit("RLIMIT SW replicated_sw 2 600", false, 2, 0, 600)
	slave.Close()
	slaveClosed = true

	recoveredFromReplicatedWAL := startTestDatabase(t, slaveWALDir)
	defer recoveredFromReplicatedWAL.Close()
	recoveredFromReplicatedWAL.RequireRateLimit("RLIMIT TB replicated_tb 3 1 600", true, 3, 0, 600)
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

func TestTCPDatabaseQPStreamFiltersQuotaEventsByPrefix(t *testing.T) {
	app := startTestDatabase(t, t.TempDir())
	defer app.Close()

	streamClient := connectEventually(t, app.address)

	events := make(chan string, 4)
	errs := make(chan error, 1)
	go func() {
		errs <- streamClient.Stream(context.Background(), []byte("QPSTREAM tenant_a-"), func(response []byte) error {
			events <- string(response)

			return nil
		})
	}()

	app.RequireQuotaAcquire("QUOTA ACQ tenant_b-quota 10 4 client-a", true, 4, 4, 6, 0)
	requireNoStreamEvent(t, events)

	app.RequireQuotaAcquire("QUOTA ACQ tenant_a-quota 10 4 client-a", true, 4, 4, 6, 0)
	require.Equal(t, "ok|acq;tenant_a-quota;client-a;4;4;6;0", requireStreamEvent(t, events))

	app.RequireQuotaAcquire("QUOTA ACQ tenant_a-quota 10 4 client-a", true, 4, 4, 6, 0)
	requireNoStreamEvent(t, events)

	app.RequireQuery("QUOTA REL tenant_a-quota client-a", "ok|1")
	require.Equal(t, "ok|rel;tenant_a-quota;client-a;4;0;10;0", requireStreamEvent(t, events))

	app.RequireQuery("QUOTA DEL tenant_a-quota", "ok|1")
	require.Equal(t, "ok|del;tenant_a-quota;;0;0;0;0", requireStreamEvent(t, events))

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
	first.RequireQuotaAcquire("QUOTA ACQ durable_quota 10 4 client-a", true, 4, 4, 6, 0)
	first.Close()

	second := startTestDatabase(t, walDir)
	defer second.Close()

	second.RequireQuery("GET durable 60", "ok|2")
	second.RequireQuery("GET limited 60", "ok|2")
	second.RequireRateLimit("RLIMIT SW sliding 2 60", false, 2, 0, 60)
	second.RequireRateLimit("RLIMIT TB bucket 3 1 600", true, 3, 0, 600)
	second.RequireQuotaInfo("QUOTA INF durable_quota", 10, 4, 6, []testQuotaClient{
		{clientID: "client-a", amount: 4},
	})
	second.RequireQuotaAcquire("QUOTA ACQ durable_quota 10 7 client-b", false, 0, 4, 6, 0)
	second.RequireQuery("QUOTA REL durable_quota client-a", "ok|1")
	second.RequireQuotaAcquire("QUOTA ACQ durable_quota 10 7 client-b", true, 7, 7, 3, 0)
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
	t               *testing.T
	address         string
	client          *network.TCPClient
	storage         *storage.Storage
	dumper          *dumper.Dumper
	walDir          string
	cancel          context.CancelFunc
	done            chan error
	replicationAddr string
	replicationStop context.CancelFunc
	replicationDone chan error
	master          *replication.Master
	logger          *zerolog.Logger
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
			return db.HandleQueryStream(ctx, string(query), write)
		})
	}()

	client := connectEventually(t, address)

	return &testDatabaseApp{
		t:       t,
		address: address,
		client:  client,
		storage: strg,
		dumper:  dumpStore,
		walDir:  walDir,
		cancel:  cancel,
		done:    done,
		logger:  &logger,
	}
}

func startTestDatabaseWithMasterReplication(t *testing.T, walDir, dumpDir, replicationAddress string) *testDatabaseApp {
	t.Helper()

	logger := zerolog.Nop()
	walStream := make(chan wal.Chunk, 8)
	dumpStream := make(chan database.DumpChunk, 1)

	engine, err := inmemory.NewEngine(inmemory.HashTableBuilder, 4, &logger, walStream, dumpStream)
	require.NoError(t, err)

	walStore := newTestWAL(walDir, walStream, &logger)
	dumpStore := dumper.New(engine, walStore, dumpDir)

	strg, err := storage.NewStorage(
		engine,
		walStore,
		dumpStore,
		nil,
		&logger,
		time.Hour,
		time.Hour,
		true,
		config.DefaultLimitEventQueueCapacity,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, strg.LoadWAL(ctx, database.NoTx))
	strg.Start(ctx)

	app := startQueryServer(t, ctx, cancel, strg, dumpStore, &logger)
	app.walDir = walDir
	app.replicationAddr = replicationAddress
	app.logger = &logger
	app.StartReplication()

	return app
}

func startTestDatabaseWithSlaveReplication(t *testing.T, walDir, replicationAddress string) *testDatabaseApp {
	t.Helper()

	logger := zerolog.Nop()
	walStream := make(chan wal.Chunk, 8)
	dumpStream := make(chan database.DumpChunk, 1)

	engine, err := inmemory.NewEngine(inmemory.HashTableBuilder, 4, &logger, walStream, dumpStream)
	require.NoError(t, err)

	walStore := newTestWAL(walDir, walStream, &logger)
	clientFactory := replication.NewTCPClientFactory(replicationAddress, 16<<20, time.Second)
	slave, err := replication.NewSlaveWithFactory(
		clientFactory,
		"replica-test",
		wal.NewFSReader(walDir, &logger),
		walStream,
		dumpStream,
		walDir,
		10*time.Millisecond,
		&logger,
	)
	require.NoError(t, err)

	strg, err := storage.NewStorage(
		engine,
		walStore,
		nil,
		slave,
		&logger,
		time.Hour,
		time.Hour,
		true,
		config.DefaultLimitEventQueueCapacity,
	)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, strg.LoadWAL(ctx, database.NoTx))
	strg.Start(ctx)

	return startQueryServer(t, ctx, cancel, strg, nil, &logger)
}

func startQueryServer(
	t *testing.T,
	ctx context.Context,
	cancel context.CancelFunc,
	strg *storage.Storage,
	dumpStore *dumper.Dumper,
	logger *zerolog.Logger,
) *testDatabaseApp {
	t.Helper()

	comp := compute.NewCompute(compute.NewParser(logger), compute.NewAnalyzer(logger), logger)
	db := database.NewDatabase(comp, strg, logger, 64<<10)
	address := freeLocalAddress(t)
	server, err := network.NewTCPServer(address, 128, 64<<10, time.Second, logger)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		done <- server.HandleQueryStreams(ctx, func(
			ctx context.Context,
			query []byte,
			write func([]byte) error,
		) error {
			return db.HandleQueryStream(ctx, string(query), write)
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
		logger:  logger,
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

	require.Equal(a.t, expected, a.Query(query))
}

func (a *testDatabaseApp) Query(query string) string {
	a.t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	response, err := a.client.Send(ctx, []byte(query))
	require.NoError(a.t, err)

	return string(response)
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

func (a *testDatabaseApp) RequireQuotaAcquire(
	query string,
	acquired bool,
	allocated database.ValueType,
	used database.ValueType,
	remaining database.ValueType,
	maxExpiresAfter uint32,
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
	require.Len(a.t, fields, 5)
	if acquired {
		require.Equal(a.t, "1", fields[0])
	} else {
		require.Equal(a.t, "0", fields[0])
	}
	require.Equal(a.t, strconv.FormatInt(int64(allocated), 10), fields[1])
	require.Equal(a.t, strconv.FormatInt(int64(used), 10), fields[2])
	require.Equal(a.t, strconv.FormatInt(int64(remaining), 10), fields[3])

	expiresAfter, err := strconv.ParseUint(fields[4], 10, 32)
	require.NoError(a.t, err)
	require.LessOrEqual(a.t, uint32(expiresAfter), maxExpiresAfter)
}

type testQuotaClient struct {
	clientID string
	amount   database.ValueType
	expires  bool
}

func (a *testDatabaseApp) RequireQuotaInfo(
	query string,
	limit database.ValueType,
	used database.ValueType,
	remaining database.ValueType,
	clients []testQuotaClient,
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
	require.Len(a.t, fields, 3+len(clients)*3)
	require.Equal(a.t, strconv.FormatInt(int64(limit), 10), fields[0])
	require.Equal(a.t, strconv.FormatInt(int64(used), 10), fields[1])
	require.Equal(a.t, strconv.FormatInt(int64(remaining), 10), fields[2])

	for i, client := range clients {
		offset := 3 + i*3
		require.Equal(a.t, client.clientID, fields[offset])
		require.Equal(a.t, strconv.FormatInt(int64(client.amount), 10), fields[offset+1])

		expiresAt, err := strconv.ParseUint(fields[offset+2], 10, 32)
		require.NoError(a.t, err)
		if client.expires {
			require.NotZero(a.t, expiresAt)
		} else {
			require.Zero(a.t, expiresAt)
		}
	}
}

func (a *testDatabaseApp) MinReplicaAckLSN() uint64 {
	a.t.Helper()

	require.NotNil(a.t, a.master)
	lsn, ok := a.master.MinReplicaAckLSN()
	if !ok {
		return 0
	}

	return lsn
}

func (a *testDatabaseApp) StartReplication() {
	a.t.Helper()

	require.NotEmpty(a.t, a.replicationAddr)
	require.NotEmpty(a.t, a.walDir)
	require.NotNil(a.t, a.dumper)
	require.NotNil(a.t, a.logger)
	require.Nil(a.t, a.replicationStop)

	replicationServer, err := network.NewTCPServer(a.replicationAddr, 5, 16<<20, time.Second, a.logger)
	require.NoError(a.t, err)
	master, err := replication.NewMaster(replicationServer, a.walDir, a.dumper, a.logger)
	require.NoError(a.t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- master.Start(ctx)
	}()

	a.master = master
	a.replicationStop = cancel
	a.replicationDone = done
}

func (a *testDatabaseApp) StopReplication() {
	a.t.Helper()

	if a.replicationStop == nil {
		return
	}

	a.replicationStop()
	a.replicationStop = nil

	select {
	case err := <-a.replicationDone:
		require.NoError(a.t, err)
	case <-time.After(time.Second):
		a.t.Fatal("replication server did not stop")
	}
	a.replicationDone = nil
	a.master = nil
}

type referenceModel struct {
	counters map[referenceKey]int
	sliding  map[referenceKey]int
	buckets  map[referenceKey]referenceTokenBucket
}

type referenceTokenBucket struct {
	used     int
	capacity int
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

type asyncQueryResult struct {
	response string
	err      error
}

func newReferenceModel() *referenceModel {
	return &referenceModel{
		counters: make(map[referenceKey]int),
		sliding:  make(map[referenceKey]int),
		buckets:  make(map[referenceKey]referenceTokenBucket),
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
		_, slidingFound := m.sliding[key]
		_, bucketFound := m.buckets[key]
		delete(m.counters, key)
		delete(m.sliding, key)
		delete(m.buckets, key)

		if counterFound || slidingFound || bucketFound {
			return expectedResponse{raw: "ok|1"}
		}

		return expectedResponse{raw: "ok|0"}
	case "MDEL":
		results := make([]string, 0, len(parts)/2)
		for i := 1; i < len(parts); i += 2 {
			key := referenceKey{key: parts[i], window: parts[i+1]}
			_, counterFound := m.counters[key]
			_, slidingFound := m.sliding[key]
			_, bucketFound := m.buckets[key]
			delete(m.counters, key)
			delete(m.sliding, key)
			delete(m.buckets, key)
			if counterFound || slidingFound || bucketFound {
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
			bucket := m.buckets[key]
			if bucket.capacity != limit {
				bucket.capacity = limit
			}
			if bucket.used >= limit {
				return expectedResponse{rateLimit: &expectedRateLimit{
					allowed:   false,
					current:   bucket.used,
					remaining: 0,
					window:    uint32(parsedWindow),
				}}
			}

			bucket.used++
			m.buckets[key] = bucket

			return expectedResponse{rateLimit: &expectedRateLimit{
				allowed:   true,
				current:   bucket.used,
				remaining: limit - bucket.used,
				window:    uint32(parsedWindow),
			}}
		}

		state := m.counters
		if algorithm == "SW" {
			state = m.sliding
		}
		current := state[key]
		if current >= limit {
			return expectedResponse{rateLimit: &expectedRateLimit{
				allowed:   false,
				current:   current,
				remaining: 0,
				window:    uint32(parsedWindow),
			}}
		}

		current++
		state[key] = current

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

func randomModelQuery(rng *rand.Rand) string {
	const window = "1000000000"
	switch rng.Intn(10) {
	case 0, 1, 2:
		return fmt.Sprintf("INCR %s %s", randomKey(rng, "counter"), window)
	case 3:
		return fmt.Sprintf("GET %s %s", randomReadableKey(rng), window)
	case 4:
		return fmt.Sprintf("DEL %s %s", randomAnyKey(rng), window)
	case 5:
		return fmt.Sprintf(
			"MDEL %s %s %s %s",
			randomAnyKey(rng),
			window,
			randomAnyKey(rng),
			window,
		)
	case 6, 7:
		return fmt.Sprintf("RLIMIT FW %s 5 %s", randomKey(rng, "fw"), window)
	case 8:
		return fmt.Sprintf("RLIMIT SW %s 5 %s", randomKey(rng, "sw"), window)
	default:
		return fmt.Sprintf("RLIMIT TB %s 5 1 %s", randomKey(rng, "tb"), window)
	}
}

func randomReadableKey(rng *rand.Rand) string {
	if rng.Intn(2) == 0 {
		return randomKey(rng, "counter")
	}

	return randomKey(rng, "fw")
}

func randomAnyKey(rng *rand.Rand) string {
	prefixes := []string{"counter", "fw", "sw", "tb"}

	return randomKey(rng, prefixes[rng.Intn(len(prefixes))])
}

func randomKey(rng *rand.Rand, prefix string) string {
	return fmt.Sprintf("%s_%02d", prefix, rng.Intn(12))
}

func requireReferenceModelSamples(t *testing.T, app *testDatabaseApp, model *referenceModel, label string) {
	t.Helper()

	require.Eventually(t, func() bool {
		for key, value := range model.counters {
			query := fmt.Sprintf("GET %s %s", key.key, key.window)
			if app.Query(query) != fmt.Sprintf("ok|%d", value) {
				return false
			}
		}

		return true
	}, time.Second, 10*time.Millisecond, label)

	slidingQueries := make([]string, 0, len(model.sliding))
	for key, value := range model.sliding {
		if value == 0 {
			continue
		}
		slidingQueries = append(slidingQueries, fmt.Sprintf("RLIMIT SW %s 5 %s", key.key, key.window))
	}
	for _, query := range slidingQueries {
		expected := model.Apply(t, query)
		actual := app.RequireOK(query)
		requireModelResponse(t, label+" query="+query, expected, actual)
	}

	tokenBucketQueries := make([]string, 0, len(model.buckets))
	for key, bucket := range model.buckets {
		if bucket.used == 0 {
			continue
		}
		capacity := bucket.capacity
		if capacity == 0 {
			capacity = 5
		}
		tokenBucketQueries = append(tokenBucketQueries, fmt.Sprintf("RLIMIT TB %s %d 1 %s", key.key, capacity, key.window))
	}
	for _, query := range tokenBucketQueries {
		expected := model.Apply(t, query)
		actual := app.RequireOK(query)
		requireModelResponse(t, label+" query="+query, expected, actual)
	}
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

func sendQueryAsync(client *network.TCPClient, query string, timeout time.Duration) <-chan asyncQueryResult {
	result := make(chan asyncQueryResult, 1)
	go func() {
		defer close(result)
		defer func() {
			_ = client.Close()
		}()

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		response, err := client.Send(ctx, []byte(query))
		result <- asyncQueryResult{response: string(response), err: err}
	}()

	return result
}

func requireAsyncResponse(t *testing.T, result <-chan asyncQueryResult, expected string) {
	t.Helper()

	select {
	case res := <-result:
		require.NoError(t, res.err)
		require.Equal(t, expected, res.response)
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for async response %q", expected)
	}
}

func requireAsyncTimeout(t *testing.T, result <-chan asyncQueryResult) {
	t.Helper()

	select {
	case res := <-result:
		require.True(
			t,
			errors.Is(res.err, context.DeadlineExceeded) || errors.Is(res.err, network.ErrIdleTimeout),
			"expected timeout, got response=%q err=%v",
			res.response,
			res.err,
		)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for async timeout")
	}
}

func (a *testDatabaseApp) Close() {
	a.t.Helper()

	require.NoError(a.t, a.client.Close())
	a.StopReplication()
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

	return connectEventuallyWithIdle(t, address, time.Second)
}

func connectEventuallyWithIdle(t *testing.T, address string, idleTimeout time.Duration) *network.TCPClient {
	t.Helper()

	var client *network.TCPClient
	require.Eventually(t, func() bool {
		var err error
		client, err = network.NewTCPClient(address, 64<<10, idleTimeout)

		return err == nil
	}, time.Second, 10*time.Millisecond)

	return client
}

func tryQuery(address, query string) (string, error) {
	client, err := network.NewTCPClient(address, 64<<10, time.Second)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = client.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	response, err := client.Send(ctx, []byte(query))
	if err != nil {
		return "", err
	}

	return string(response), nil
}

func waitTCPAddress(t *testing.T, address string, maxMessageSize int) {
	t.Helper()

	var client *network.TCPClient
	require.Eventually(t, func() bool {
		var err error
		client, err = network.NewTCPClient(address, maxMessageSize, time.Second)
		return err == nil
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, client.Close())
}
