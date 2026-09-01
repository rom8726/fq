package stress

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestSignalServerProcessSignalsChildProcesses(t *testing.T) {
	dir := t.TempDir()
	scriptPath := filepath.Join(dir, "server-wrapper.sh")
	childMarkerPath := filepath.Join(dir, "child-term")
	childReadyPath := filepath.Join(dir, "child-ready")
	script := `#!/bin/sh
trap 'exit 0' TERM
( trap 'echo child > "$1"; exit 0' TERM; echo ready > "$2"; while :; do sleep 1; done ) &
wait
`
	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	cmd := exec.CommandContext(context.Background(), scriptPath, childMarkerPath, childReadyPath)
	configureServerCommand(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	requireFileEventually(t, childReadyPath)
	if err := signalServerProcess(cmd, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()

	requireFileEventually(t, childMarkerPath)
}

func requireFileEventually(t *testing.T, path string) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("file %s was not created", path)
}
