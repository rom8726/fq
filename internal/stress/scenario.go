package stress

import (
	"context"
	"fmt"
)

const RestartSmokeScenario = "restart-smoke"

type Result struct {
	Scenario   string
	Address    string
	RootDir    string
	Operations int
}

func Run(ctx context.Context, opts Options) (Result, error) {
	if opts.Scenario == "" {
		opts.Scenario = RestartSmokeScenario
	}
	if opts.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Duration)
		defer cancel()
	}

	switch opts.Scenario {
	case RestartSmokeScenario:
		return RunRestartSmoke(ctx, opts)
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
