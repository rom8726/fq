package stress

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	RestartSmokeScenario = "restart-smoke"
	CrashLoopScenario    = "crash-loop"
	counterWindow        = 600
)

type Result struct {
	Scenario        string
	Address         string
	RootDir         string
	Operations      int
	Restarts        int
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
	default:
		return Result{}, fmt.Errorf("unknown scenario %q", opts.Scenario)
	}
}

func RunRestartSmoke(ctx context.Context, opts Options) (Result, error) {
	env, err := NewEnvironment(opts)
	if err != nil {
		return Result{}, err
	}
	if !opts.KeepData {
		defer func() { _ = env.Cleanup() }()
	}

	server, err := StartServer(ctx, env)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = server.Kill() }()

	verifier := NewVerifier(env.Address, env.MaxMessageSize, env.IdleTimeout)
	for i := 0; i < 10; i++ {
		if err := verifier.ExpectOK(ctx, "INCR stress_counter 600"); err != nil {
			return Result{}, fmt.Errorf("write before restart: %w", err)
		}
	}

	if err := server.Restart(ctx); err != nil {
		return Result{}, fmt.Errorf("restart server: %w", err)
	}

	verifier = NewVerifier(env.Address, env.MaxMessageSize, env.IdleTimeout)
	if err := verifier.ExpectValue(ctx, "stress_counter", 600, 10); err != nil {
		return Result{}, fmt.Errorf("verify after restart: %w", err)
	}

	return Result{
		Scenario:   RestartSmokeScenario,
		Address:    env.Address,
		RootDir:    env.RootDir,
		Operations: 10,
	}, nil
}

func RunCrashLoop(ctx context.Context, opts Options) (Result, error) {
	opts = normalizeCrashLoopOptions(opts)
	env, err := NewEnvironment(opts)
	if err != nil {
		return Result{}, err
	}
	if !opts.KeepData {
		defer func() { _ = env.Cleanup() }()
	}

	server, err := StartServer(ctx, env)
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = server.Kill() }()

	var expectedMu sync.Mutex
	expected := make(map[string]uint64, opts.Keys)
	var operations atomic.Int64
	var transientErrors atomic.Int64

	workCtx, stopWork := context.WithTimeout(ctx, opts.Duration)
	defer stopWork()

	var wg sync.WaitGroup
	for id := 0; id < opts.Workers; id++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runCrashLoopWorker(workCtx, env, opts, workerID, expected, &expectedMu, &operations, &transientErrors)
		}(id)
	}

	restarts, err := runCrashLoopRestarts(workCtx, ctx, server, opts.KillInterval)
	stopWork()
	wg.Wait()
	if err != nil {
		return Result{}, err
	}

	if err := server.Restart(ctx); err != nil {
		return Result{}, fmt.Errorf("final restart server: %w", err)
	}
	if err := verifyExpectedCounters(ctx, env, expected, &expectedMu); err != nil {
		return Result{}, err
	}

	return Result{
		Scenario:        CrashLoopScenario,
		Address:         env.Address,
		RootDir:         env.RootDir,
		Operations:      int(operations.Load()),
		Restarts:        restarts + 1,
		TransientErrors: int(transientErrors.Load()),
	}, nil
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

func runCrashLoopWorker(
	ctx context.Context,
	env *Environment,
	opts Options,
	workerID int,
	expected map[string]uint64,
	expectedMu *sync.Mutex,
	operations *atomic.Int64,
	transientErrors *atomic.Int64,
) {
	rng := rand.New(rand.NewSource(opts.Seed + int64(workerID)*7919)) //nolint:gosec // deterministic stress seed.

	for ctx.Err() == nil {
		key := fmt.Sprintf("stress_counter_%03d", rng.Intn(opts.Keys))
		requestCtx, cancel := context.WithTimeout(ctx, opts.RequestTimeout)
		response, err := NewVerifier(env.Address, env.MaxMessageSize, env.IdleTimeout).
			Query(requestCtx, fmt.Sprintf("INCR %s %d", key, counterWindow))
		cancel()
		if err != nil {
			transientErrors.Add(1)
			sleepOrDone(ctx, 10*time.Millisecond)

			continue
		}

		value, ok := parseOKUint(response)
		if !ok {
			transientErrors.Add(1)

			continue
		}

		expectedMu.Lock()
		if value > expected[key] {
			expected[key] = value
		}
		expectedMu.Unlock()
		operations.Add(1)
	}
}

func runCrashLoopRestarts(
	workCtx, serverCtx context.Context,
	server *ServerProcess,
	interval time.Duration,
) (int, error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	restarts := 0
	for {
		select {
		case <-workCtx.Done():
			return restarts, nil
		case <-ticker.C:
			if err := server.Restart(serverCtx); err != nil {
				return restarts, fmt.Errorf("restart server: %w", err)
			}
			restarts++
		}
	}
}

func verifyExpectedCounters(
	ctx context.Context,
	env *Environment,
	expected map[string]uint64,
	expectedMu *sync.Mutex,
) error {
	expectedMu.Lock()
	snapshot := make(map[string]uint64, len(expected))
	for key, value := range expected {
		snapshot[key] = value
	}
	expectedMu.Unlock()

	verifier := NewVerifier(env.Address, env.MaxMessageSize, env.IdleTimeout)
	for key, want := range snapshot {
		if err := verifier.ExpectValueAtLeast(ctx, key, counterWindow, want); err != nil {
			return fmt.Errorf("verify expected counter %s: %w", key, err)
		}
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
