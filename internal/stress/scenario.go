package stress

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	RestartSmokeScenario       = "restart-smoke"
	CrashLoopScenario          = "crash-loop"
	DumpRecoveryScenario       = "dump-recovery"
	ReplicationStressScenario  = "replication-stress"
	counterWindow              = 600
	defaultConvergenceDeadline = 10 * time.Second
	eventServerReady           = "server_ready"
)

type Result struct {
	Scenario        string
	Address         string
	SlaveAddress    string
	RootDir         string
	ReportPath      string
	Operations      int
	Restarts        int
	Dumps           int
	TransientErrors int
}

func Run(ctx context.Context, opts Options) (Result, error) {
	if opts.Scenario == "" {
		opts.Scenario = RestartSmokeScenario
	}

	switch opts.Scenario {
	case RestartSmokeScenario:
		if opts.Duration > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, opts.Duration)
			defer cancel()
		}

		return RunRestartSmoke(ctx, opts)
	case CrashLoopScenario:
		return RunCrashLoop(ctx, opts)
	case DumpRecoveryScenario:
		return RunDumpRecovery(ctx, opts)
	case ReplicationStressScenario:
		return RunReplicationStress(ctx, opts)
	default:
		return Result{}, fmt.Errorf("unknown scenario %q", opts.Scenario)
	}
}

func RunRestartSmoke(ctx context.Context, opts Options) (result Result, runErr error) {
	if opts.Scenario == "" {
		opts.Scenario = RestartSmokeScenario
	}
	env, err := NewEnvironment(opts)
	if err != nil {
		return Result{}, err
	}
	startedAt := time.Now()
	events := NewEventLog(defaultEventLimit)
	defer func() {
		runErr = finishScenario(opts, env, startedAt, &result, runErr, events, nil)
	}()

	server, err := StartServer(ctx, env)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = server.Kill() }()
	events.Add(Event{Kind: eventServerReady})

	verifier := NewVerifier(env.Address, env.MaxMessageSize, env.IdleTimeout)
	for i := 0; i < 10; i++ {
		query := "INCR stress_counter 600"
		if err := verifier.ExpectOK(ctx, query); err != nil {
			events.Add(Event{Kind: "write_error", Query: query, Error: err.Error()})

			return Result{}, fmt.Errorf("write before restart: %w", err)
		}
		events.Add(Event{Kind: "write_ok", Key: "stress_counter", Query: query})
	}

	if err := server.Restart(ctx); err != nil {
		events.Add(Event{Kind: "restart_error", Error: err.Error()})

		return Result{}, fmt.Errorf("restart server: %w", err)
	}
	events.Add(Event{Kind: "restart_ok"})

	verifier = NewVerifier(env.Address, env.MaxMessageSize, env.IdleTimeout)
	if err := verifier.ExpectValue(ctx, "stress_counter", 600, 10); err != nil {
		events.Add(Event{Kind: "verify_error", Key: "stress_counter", Error: err.Error()})

		return Result{}, fmt.Errorf("verify after restart: %w", err)
	}

	result = Result{
		Scenario:   RestartSmokeScenario,
		Address:    env.Address,
		RootDir:    env.RootDir,
		ReportPath: env.ReportPath,
		Operations: 10,
	}

	return result, nil
}

func RunCrashLoop(ctx context.Context, opts Options) (result Result, runErr error) {
	if opts.Scenario == "" {
		opts.Scenario = CrashLoopScenario
	}
	opts = normalizeCrashLoopOptions(opts)
	env, err := NewEnvironment(opts)
	if err != nil {
		return Result{}, err
	}
	startedAt := time.Now()
	events := NewEventLog(defaultEventLimit)

	var expectedMu sync.Mutex
	expected := make(map[string]uint64, opts.Keys)
	defer func() {
		runErr = finishScenario(opts, env, startedAt, &result, runErr, events, func() map[string]uint64 {
			return expectedSnapshot(expected, &expectedMu)
		})
	}()

	server, err := StartServer(ctx, env)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = server.Kill() }()
	events.Add(Event{Kind: eventServerReady})

	var operations atomic.Int64
	var transientErrors atomic.Int64

	workCtx, stopWork := context.WithTimeout(ctx, opts.Duration)
	defer stopWork()

	var wg sync.WaitGroup
	for id := 0; id < opts.Workers; id++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runCrashLoopWorker(workCtx, env, opts, workerID, expected, &expectedMu, &operations, &transientErrors, events)
		}(id)
	}

	restarts, err := runCrashLoopRestarts(workCtx, ctx, server, opts.KillInterval, events)
	stopWork()
	wg.Wait()
	if err != nil {
		return Result{}, err
	}

	if err := server.Restart(ctx); err != nil {
		events.Add(Event{Kind: "final_restart_error", Error: err.Error()})

		return Result{}, fmt.Errorf("final restart server: %w", err)
	}
	events.Add(Event{Kind: "final_restart_ok"})

	if err := verifyExpectedCounters(ctx, env, expected, &expectedMu, events); err != nil {
		return Result{}, err
	}

	result = Result{
		Scenario:        CrashLoopScenario,
		Address:         env.Address,
		RootDir:         env.RootDir,
		ReportPath:      env.ReportPath,
		Operations:      int(operations.Load()),
		Restarts:        restarts + 1,
		TransientErrors: int(transientErrors.Load()),
	}

	return result, nil
}

func RunDumpRecovery(ctx context.Context, opts Options) (result Result, runErr error) {
	if opts.Scenario == "" {
		opts.Scenario = DumpRecoveryScenario
	}
	opts = normalizeDumpRecoveryOptions(opts)
	env, err := NewEnvironment(opts)
	if err != nil {
		return Result{}, err
	}
	startedAt := time.Now()
	events := NewEventLog(defaultEventLimit)

	var expectedMu sync.Mutex
	expected := make(map[string]uint64, opts.Keys)
	defer func() {
		runErr = finishScenario(opts, env, startedAt, &result, runErr, events, func() map[string]uint64 {
			return expectedSnapshot(expected, &expectedMu)
		})
	}()

	server, err := StartServer(ctx, env)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = server.Kill() }()
	events.Add(Event{Kind: eventServerReady})

	var operations atomic.Int64
	var transientErrors atomic.Int64

	workCtx, stopWork := context.WithTimeout(ctx, opts.Duration)
	defer stopWork()

	var wg sync.WaitGroup
	for id := 0; id < opts.Workers; id++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runCrashLoopWorker(workCtx, env, opts, workerID, expected, &expectedMu, &operations, &transientErrors, events)
		}(id)
	}

	restarts, err := runCrashLoopRestarts(workCtx, ctx, server, opts.KillInterval, events)
	stopWork()
	wg.Wait()
	if err != nil {
		return Result{}, err
	}

	dumps, err := waitForCompletedDump(ctx, env, events)
	if err != nil {
		return Result{}, err
	}

	if err := server.Restart(ctx); err != nil {
		events.Add(Event{Kind: "final_restart_error", Error: err.Error()})

		return Result{}, fmt.Errorf("final restart server: %w", err)
	}
	events.Add(Event{Kind: "final_restart_ok"})

	if err := verifyExpectedCounters(ctx, env, expected, &expectedMu, events); err != nil {
		return Result{}, err
	}

	result = Result{
		Scenario:        DumpRecoveryScenario,
		Address:         env.Address,
		RootDir:         env.RootDir,
		ReportPath:      env.ReportPath,
		Operations:      int(operations.Load()),
		Restarts:        restarts + 1,
		Dumps:           dumps,
		TransientErrors: int(transientErrors.Load()),
	}

	return result, nil
}

func RunReplicationStress(ctx context.Context, opts Options) (result Result, runErr error) {
	if opts.Scenario == "" {
		opts.Scenario = ReplicationStressScenario
	}
	opts = normalizeReplicationStressOptions(opts)
	masterEnv, slaveEnv, err := NewReplicationEnvironment(opts)
	if err != nil {
		return Result{}, err
	}
	startedAt := time.Now()
	events := NewEventLog(defaultEventLimit)

	var expectedMu sync.Mutex
	expected := make(map[string]uint64, opts.Keys)
	defer func() {
		runErr = finishScenario(opts, masterEnv, startedAt, &result, runErr, events, func() map[string]uint64 {
			return expectedSnapshot(expected, &expectedMu)
		})
	}()

	master, err := StartServer(ctx, masterEnv)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = master.Kill() }()
	events.Add(Event{Kind: "master_ready"})

	slave, err := StartServerWithReadyQuery(ctx, slaveEnv, "GET stress_ready 600")
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = slave.Kill() }()
	events.Add(Event{Kind: "slave_ready"})

	var operations atomic.Int64
	var transientErrors atomic.Int64

	workCtx, stopWork := context.WithTimeout(ctx, opts.Duration)
	defer stopWork()

	var wg sync.WaitGroup
	for id := 0; id < opts.Workers; id++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runCrashLoopWorker(workCtx, masterEnv, opts, workerID, expected, &expectedMu, &operations, &transientErrors, events)
		}(id)
	}

	restarts, err := runSlaveReconnects(workCtx, ctx, slave, opts.KillInterval, events)
	stopWork()
	wg.Wait()
	if err != nil {
		return Result{}, err
	}

	if err := waitForReplicationConvergence(ctx, slaveEnv, expected, &expectedMu, events); err != nil {
		return Result{}, err
	}

	result = Result{
		Scenario:        ReplicationStressScenario,
		Address:         masterEnv.Address,
		SlaveAddress:    slaveEnv.Address,
		RootDir:         masterEnv.RootDir,
		ReportPath:      masterEnv.ReportPath,
		Operations:      int(operations.Load()),
		Restarts:        restarts,
		TransientErrors: int(transientErrors.Load()),
	}

	return result, nil
}

func normalizeCrashLoopOptions(opts Options) Options {
	if opts.Duration <= 0 {
		opts.Duration = 30 * time.Second
	}
	if opts.Workers <= 0 {
		opts.Workers = 4
	}
	if opts.Keys <= 0 {
		opts.Keys = 100
	}
	if opts.KillInterval <= 0 {
		opts.KillInterval = 2 * time.Second
	}
	if opts.RequestTimeout <= 0 {
		opts.RequestTimeout = time.Second
	}

	return opts
}

func normalizeDumpRecoveryOptions(opts Options) Options {
	opts = normalizeCrashLoopOptions(opts)
	if opts.DumpInterval <= 0 {
		opts.DumpInterval = 250 * time.Millisecond
	}
	if opts.KillInterval <= opts.DumpInterval {
		opts.KillInterval = opts.DumpInterval * 3
	}

	return opts
}

func normalizeReplicationStressOptions(opts Options) Options {
	opts = normalizeCrashLoopOptions(opts)
	if opts.SyncInterval <= 0 {
		opts.SyncInterval = 100 * time.Millisecond
	}
	if opts.KillInterval <= opts.SyncInterval {
		opts.KillInterval = opts.SyncInterval * 3
	}

	return opts
}

func runCrashLoopWorker(
	ctx context.Context,
	env *Environment,
	opts Options,
	workerID int,
	expected map[string]uint64,
	expectedMu *sync.Mutex,
	operations *atomic.Int64,
	transientErrors *atomic.Int64,
	events *EventLog,
) {
	rng := rand.New(rand.NewSource(opts.Seed + int64(workerID)*7919)) //nolint:gosec // deterministic stress seed.

	for ctx.Err() == nil {
		key := fmt.Sprintf("stress_counter_%03d", rng.Intn(opts.Keys))
		query := fmt.Sprintf("INCR %s %d", key, counterWindow)
		requestCtx, cancel := context.WithTimeout(ctx, opts.RequestTimeout)
		response, err := NewVerifier(env.Address, env.MaxMessageSize, env.IdleTimeout).
			Query(requestCtx, query)
		cancel()
		if err != nil {
			transientErrors.Add(1)
			events.Add(Event{Kind: "write_transient_error", Worker: workerID, Key: key, Query: query, Error: err.Error()})
			sleepOrDone(ctx, 10*time.Millisecond)

			continue
		}

		value, ok := parseOKUint(response)
		if !ok {
			transientErrors.Add(1)
			events.Add(Event{Kind: "write_bad_response", Worker: workerID, Key: key, Query: query, Response: response})

			continue
		}

		expectedMu.Lock()
		if value > expected[key] {
			expected[key] = value
		}
		expectedMu.Unlock()
		operations.Add(1)
		events.Add(Event{Kind: "write_ok", Worker: workerID, Key: key, Value: value, Query: query, Response: response})
	}
}

func runCrashLoopRestarts(
	workCtx, serverCtx context.Context,
	server *ServerProcess,
	interval time.Duration,
	events *EventLog,
) (int, error) {
	return runServerRestarts(workCtx, serverCtx, server, interval, events, "restart", "restart server")
}

func runSlaveReconnects(
	workCtx, serverCtx context.Context,
	slave *ServerProcess,
	interval time.Duration,
	events *EventLog,
) (int, error) {
	return runServerRestarts(workCtx, serverCtx, slave, interval, events, "slave_restart", "restart slave")
}

func runServerRestarts(
	workCtx, serverCtx context.Context,
	server *ServerProcess,
	interval time.Duration,
	events *EventLog,
	eventPrefix string,
	errorPrefix string,
) (int, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	restarts := 0
	for {
		select {
		case <-workCtx.Done():
			return restarts, nil
		case <-ticker.C:
			events.Add(Event{Kind: eventPrefix + "_begin"})
			if err := server.Restart(serverCtx); err != nil {
				events.Add(Event{Kind: eventPrefix + "_error", Error: err.Error()})

				return restarts, fmt.Errorf("%s: %w", errorPrefix, err)
			}
			restarts++
			events.Add(Event{Kind: eventPrefix + "_ok"})
		}
	}
}

func verifyExpectedCounters(
	ctx context.Context,
	env *Environment,
	expected map[string]uint64,
	expectedMu *sync.Mutex,
	events *EventLog,
) error {
	snapshot := expectedSnapshot(expected, expectedMu)

	verifier := NewVerifier(env.Address, env.MaxMessageSize, env.IdleTimeout)
	for key, want := range snapshot {
		if err := verifier.ExpectValueAtLeast(ctx, key, counterWindow, want); err != nil {
			events.Add(Event{Kind: "verify_error", Key: key, Value: want, Error: err.Error()})

			return fmt.Errorf("verify expected counter %s: %w", key, err)
		}
		events.Add(Event{Kind: "verify_ok", Key: key, Value: want})
	}

	return nil
}

func waitForCompletedDump(ctx context.Context, env *Environment, events *EventLog) (int, error) {
	dumpPath := filepath.Join(env.DumpDir, "current.dump")
	deadlineCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		info, err := os.Stat(dumpPath)
		if err == nil && info.Size() > 0 {
			events.Add(Event{Kind: "dump_seen", Query: dumpPath, Value: uint64(info.Size())})

			return 1, nil
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			events.Add(Event{Kind: "dump_stat_error", Query: dumpPath, Error: err.Error()})

			return 0, fmt.Errorf("stat dump file: %w", err)
		}

		select {
		case <-deadlineCtx.Done():
			events.Add(Event{Kind: "dump_wait_timeout", Query: dumpPath, Error: deadlineCtx.Err().Error()})

			return 0, fmt.Errorf("wait for completed dump %s: %w", dumpPath, deadlineCtx.Err())
		case <-ticker.C:
		}
	}
}

func waitForReplicationConvergence(
	ctx context.Context,
	slaveEnv *Environment,
	expected map[string]uint64,
	expectedMu *sync.Mutex,
	events *EventLog,
) error {
	deadlineCtx, cancel := context.WithTimeout(ctx, defaultConvergenceDeadline)
	defer cancel()

	for {
		err := verifyExpectedCounters(deadlineCtx, slaveEnv, expected, expectedMu, events)
		if err == nil {
			events.Add(Event{Kind: "replication_converged"})

			return nil
		}
		events.Add(Event{Kind: "replication_not_converged", Error: err.Error()})

		select {
		case <-deadlineCtx.Done():
			return fmt.Errorf("replication convergence: %w", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func expectedSnapshot(expected map[string]uint64, expectedMu *sync.Mutex) map[string]uint64 {
	expectedMu.Lock()
	defer expectedMu.Unlock()

	snapshot := make(map[string]uint64, len(expected))
	for key, value := range expected {
		snapshot[key] = value
	}

	return snapshot
}

func finishScenario(
	opts Options,
	env *Environment,
	startedAt time.Time,
	result *Result,
	runErr error,
	events *EventLog,
	expected func() map[string]uint64,
) error {
	finishedAt := time.Now()
	status := "passed"
	failure := ""
	if runErr != nil {
		status = "failed"
		failure = runErr.Error()
	}
	if result.Scenario == "" {
		result.Scenario = opts.Scenario
	}
	result.Address = env.Address
	result.RootDir = env.RootDir
	result.ReportPath = env.ReportPath

	var expectedCounters map[string]uint64
	if expected != nil {
		expectedCounters = expected()
	}

	report := Report{
		Scenario:         result.Scenario,
		Status:           status,
		StartedAt:        startedAt,
		FinishedAt:       finishedAt,
		DurationMillis:   finishedAt.Sub(startedAt).Milliseconds(),
		Options:          opts,
		Result:           *result,
		Failure:          failure,
		Environment:      ReportEnvironmentFrom(env),
		ExpectedCounters: expectedCounters,
		LastEvents:       events.Snapshot(),
	}
	if err := WriteReport(report); err != nil {
		runErr = errors.Join(runErr, err)
	}
	if runErr != nil {
		return fmt.Errorf("%w (stress report: %s)", runErr, env.ReportPath)
	}

	if !opts.KeepData {
		_ = env.Cleanup()
	}

	return nil
}

func parseOKUint(response string) (uint64, bool) {
	if !strings.HasPrefix(response, "ok|") {
		return 0, false
	}

	value, err := strconv.ParseUint(strings.TrimPrefix(response, "ok|"), 10, 64)

	return value, err == nil
}

func sleepOrDone(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
