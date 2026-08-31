package stress

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
)

type ServerProcess struct {
	env       *Environment
	cmd       *exec.Cmd
	ready     string
	startedAt time.Time
	stdout    *os.File
	stderr    *os.File
}

func StartServer(ctx context.Context, env *Environment) (*ServerProcess, error) {
	return StartServerWithReadyQuery(ctx, env, "INCR stress_ready 600")
}

func StartServerWithReadyQuery(ctx context.Context, env *Environment, readyQuery string) (*ServerProcess, error) {
	cmd := serverCommand(ctx, env)
	stdout, err := os.OpenFile(env.StdoutPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open server stdout log: %w", err)
	}
	stderr, err := os.OpenFile(env.StderrPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		_ = stdout.Close()

		return nil, fmt.Errorf("open server stderr log: %w", err)
	}
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()

		return nil, fmt.Errorf("start fq server: %w", err)
	}

	process := &ServerProcess{
		env:       env,
		cmd:       cmd,
		ready:     readyQuery,
		startedAt: time.Now(),
		stdout:    stdout,
		stderr:    stderr,
	}
	if err := process.WaitReady(ctx); err != nil {
		_ = process.Kill()

		return nil, err
	}

	return process, nil
}

//nolint:gosec // ok
func serverCommand(ctx context.Context, env *Environment) *exec.Cmd {
	if env.FQBinary != "" {
		cmd := exec.CommandContext(ctx, env.FQBinary, env.ConfigPath)
		cmd.Dir = env.RepositoryDir
		cmd.Env = append(os.Environ(), env.ReplicationTokenEnv())

		return cmd
	}

	cmd := exec.CommandContext(ctx, "go", "run", "./cmd/fq", env.ConfigPath)
	cmd.Dir = env.RepositoryDir
	cmd.Env = append(os.Environ(), env.ReplicationTokenEnv())

	return cmd
}

func (p *ServerProcess) WaitReady(ctx context.Context) error {
	verifier := NewVerifier(p.env.Address, p.env.MaxMessageSize, p.env.IdleTimeout)
	deadlineCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var lastErr error
loop:
	for deadlineCtx.Err() == nil {
		err := verifier.ExpectOK(deadlineCtx, p.ready)
		if err == nil {
			return nil
		}

		lastErr = err

		select {
		case <-deadlineCtx.Done():
			break loop
		case <-time.After(50 * time.Millisecond):
		}
	}

	return fmt.Errorf("server did not become ready at %s: %w", p.env.Address, lastErr)
}

func (p *ServerProcess) Restart(ctx context.Context) error {
	if err := p.Kill(); err != nil {
		return err
	}

	next, err := StartServerWithReadyQuery(ctx, p.env, p.ready)
	if err != nil {
		return err
	}

	*p = *next

	return nil
}

func (p *ServerProcess) Kill() error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return nil
	}

	err := p.cmd.Process.Kill()
	if err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("kill fq server: %w", err)
	}

	if waitErr := p.cmd.Wait(); waitErr != nil {
		var exitErr *exec.ExitError
		if !errors.As(waitErr, &exitErr) {
			return fmt.Errorf("wait killed fq server: %w", waitErr)
		}
	}

	return p.closeLogs()
}

func (p *ServerProcess) StopGracefully(ctx context.Context) error {
	if p == nil || p.cmd == nil || p.cmd.Process == nil {
		return nil
	}

	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("stop fq server: %w", err)
	}

	done := make(chan error, 1)
	go func() {
		done <- p.cmd.Wait()
	}()

	select {
	case <-ctx.Done():
		_ = p.cmd.Process.Kill()
		<-done

		return ctx.Err()
	case err := <-done:
		if err != nil {
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) {
				return fmt.Errorf("wait stopped fq server: %w", err)
			}
		}

		return p.closeLogs()
	}
}

func (p *ServerProcess) closeLogs() error {
	var err error
	if p.stdout != nil {
		err = errors.Join(err, p.stdout.Close())
		p.stdout = nil
	}
	if p.stderr != nil {
		err = errors.Join(err, p.stderr.Close())
		p.stderr = nil
	}

	return err
}
