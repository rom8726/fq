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

func TestNewReplicationEnvironmentWritesMasterAndSlaveConfigs(t *testing.T) {
	rootDir := t.TempDir()
	master, slave, err := NewReplicationEnvironment(Options{
		Seed:         11,
		WorkDir:      rootDir,
		SyncInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	if master.RootDir != rootDir || slave.RootDir != rootDir {
		t.Fatalf("root dirs = %q/%q, want %q", master.RootDir, slave.RootDir, rootDir)
	}
	if master.Address == slave.Address {
		t.Fatalf("master and slave query addresses should differ: %q", master.Address)
	}
	if master.MasterAddress == "" || master.MasterAddress != slave.MasterAddress {
		t.Fatalf("replication addresses = %q/%q", master.MasterAddress, slave.MasterAddress)
	}
	if master.ReplicationToken == "" || master.ReplicationToken != slave.ReplicationToken {
		t.Fatalf("replication tokens = %q/%q, want shared non-empty token", master.ReplicationToken, slave.ReplicationToken)
	}

	masterConfig, err := os.ReadFile(master.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	slaveConfig, err := os.ReadFile(slave.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		text string
		want []string
	}{
		{
			name: "master",
			text: string(masterConfig),
			want: []string{`replica_type: master`, `sync_interval: 50ms`, master.MasterAddress},
		},
		{
			name: "slave",
			text: string(slaveConfig),
			want: []string{`replica_type: slave`, `replica_id: "stress-replica-1"`, `sync_interval: 50ms`, slave.MasterAddress},
		},
	} {
		for _, want := range tc.want {
			if !strings.Contains(tc.text, want) {
				t.Fatalf("%s config does not contain %q:\n%s", tc.name, want, tc.text)
			}
		}
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

func TestNormalizeDumpRecoveryOptionsDefaults(t *testing.T) {
	opts := normalizeDumpRecoveryOptions(Options{})

	if opts.DumpInterval != 250*time.Millisecond {
		t.Fatalf("dump interval = %s", opts.DumpInterval)
	}
	if opts.KillInterval <= opts.DumpInterval {
		t.Fatalf("kill interval %s should be greater than dump interval %s", opts.KillInterval, opts.DumpInterval)
	}
}

func TestNormalizeReplicationStressOptionsDefaults(t *testing.T) {
	opts := normalizeReplicationStressOptions(Options{})

	if opts.SyncInterval != 100*time.Millisecond {
		t.Fatalf("sync interval = %s", opts.SyncInterval)
	}
	if opts.KillInterval <= opts.SyncInterval {
		t.Fatalf("kill interval %s should be greater than sync interval %s", opts.KillInterval, opts.SyncInterval)
	}
}

func TestParseOKUint(t *testing.T) {
	value, ok := parseOKUint("42")
	if !ok || value != 42 {
		t.Fatalf("parse ok value = %d/%v", value, ok)
	}

	for _, response := range []string{"bad", "", "nope"} {
		if _, ok := parseOKUint(response); ok {
			t.Fatalf("parsed invalid response %q", response)
		}
	}
}

func TestWaitForCompletedDump(t *testing.T) {
	env, err := NewEnvironment(Options{WorkDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}

	dumpPath := filepath.Join(env.DumpDir, "current.dump")
	if err := os.WriteFile(dumpPath, []byte("dump"), 0o600); err != nil {
		t.Fatal(err)
	}

	events := NewEventLog(10)
	dumps, err := waitForCompletedDump(t.Context(), env, events)
	if err != nil {
		t.Fatal(err)
	}
	if dumps != 1 {
		t.Fatalf("dumps = %d", dumps)
	}
	if got := events.Snapshot(); len(got) != 1 || got[0].Kind != "dump_seen" {
		t.Fatalf("unexpected events: %+v", got)
	}
}
