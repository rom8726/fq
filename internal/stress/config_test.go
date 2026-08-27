package stress

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNewEnvironmentWritesIsolatedConfig(t *testing.T) {
	workDir := t.TempDir()
	env, err := NewEnvironment(Options{
		Seed:          7,
		WorkDir:       workDir,
		RepositoryDir: ".",
	})
	if err != nil {
		t.Fatal(err)
	}

	if env.RootDir != workDir {
		t.Fatalf("root dir = %q", env.RootDir)
	}
	if env.MaxMessageSize != defaultMaxMessageSize {
		t.Fatalf("max message size = %d", env.MaxMessageSize)
	}
	if env.IdleTimeout != defaultIdleTimeout {
		t.Fatalf("idle timeout = %s", env.IdleTimeout)
	}

	data, err := os.ReadFile(env.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, want := range []string{
		`mode: wal_and_dump`,
		`sync_commit: on`,
		`replication: {}`,
		`address: "`,
		env.WALDir,
		env.DumpDir,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("config does not contain %q:\n%s", want, text)
		}
	}
}

func TestEnvironmentCleanupRemovesRootDir(t *testing.T) {
	parent := t.TempDir()
	workDir := filepath.Join(parent, "stress")
	env, err := NewEnvironment(Options{WorkDir: workDir})
	if err != nil {
		t.Fatal(err)
	}

	if err := env.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(workDir); !os.IsNotExist(err) {
		t.Fatalf("work dir still exists or stat failed unexpectedly: %v", err)
	}
}

func TestRunRejectsUnknownScenario(t *testing.T) {
	_, err := Run(t.Context(), Options{Scenario: "missing", Duration: time.Second})
	if err == nil {
		t.Fatal("expected unknown scenario error")
	}
}

func TestNormalizeCrashLoopOptionsDefaults(t *testing.T) {
	opts := normalizeCrashLoopOptions(Options{})

	if opts.Duration != 30*time.Second {
		t.Fatalf("duration = %s", opts.Duration)
	}
	if opts.Workers != 4 {
		t.Fatalf("workers = %d", opts.Workers)
	}
	if opts.Keys != 100 {
		t.Fatalf("keys = %d", opts.Keys)
	}
	if opts.KillInterval != 2*time.Second {
		t.Fatalf("kill interval = %s", opts.KillInterval)
	}
	if opts.RequestTimeout != time.Second {
		t.Fatalf("request timeout = %s", opts.RequestTimeout)
	}
}

func TestParseOKUint(t *testing.T) {
	value, ok := parseOKUint("ok|42")
	if !ok || value != 42 {
		t.Fatalf("parse ok value = %d/%v", value, ok)
	}

	for _, response := range []string{"err|bad", "ok|", "ok|nope"} {
		if _, ok := parseOKUint(response); ok {
			t.Fatalf("parsed invalid response %q", response)
		}
	}
}
