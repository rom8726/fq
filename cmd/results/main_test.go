package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestBuildCommandsSmokeUsesShortOverrides(t *testing.T) {
	paths := artifacts{
		BenchDir:  filepath.Join("runs", "smoke", "benchmarks"),
		StressDir: filepath.Join("runs", "smoke", "stress"),
	}

	commands := buildCommands(config{
		mode:              modeSmoke,
		address:           ":1945",
		includeBenchmarks: true,
		includeStress:     true,
	}, paths)

	if len(commands) != 4 {
		t.Fatalf("commands = %d, want 4", len(commands))
	}

	bench := commands[0]
	if bench.Name != "bench-smoke" || bench.OutputFile == "" {
		t.Fatalf("unexpected bench command: %+v", bench)
	}
	joined := strings.Join(bench.Command, " ")
	for _, want := range []string{"-warmup 1s", "-duration 3s", "-connections 8", "-key_range 1000"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("bench command %q does not contain %q", joined, want)
		}
	}

	for _, command := range commands[1:] {
		if command.Kind != "stress" || command.Duration != "3s" {
			t.Fatalf("unexpected stress command: %+v", command)
		}
	}
}

func TestBuildCommandsReleaseIncludesAllProfiles(t *testing.T) {
	commands := buildCommands(config{
		mode:              modeRelease,
		address:           ":1945",
		includeBenchmarks: true,
		includeStress:     true,
	}, artifacts{
		BenchDir:  filepath.Join("runs", "release", "benchmarks"),
		StressDir: filepath.Join("runs", "release", "stress"),
	})

	if len(commands) != 9 {
		t.Fatalf("commands = %d, want 9", len(commands))
	}
	if commands[0].Name != "bench-release-hot-counter" {
		t.Fatalf("first command = %q", commands[0].Name)
	}
	if commands[len(commands)-1].Name != "stress-replication-stress" {
		t.Fatalf("last command = %q", commands[len(commands)-1].Name)
	}
}

func TestCreateArtifactsUsesStableRunID(t *testing.T) {
	meta := metadata{
		Mode:        modeSmoke,
		GitCommit:   "1234567890abcdef",
		Machine:     "test-machine",
		GeneratedAt: time.Date(2026, 8, 27, 12, 30, 0, 0, time.UTC),
	}

	paths, err := createArtifacts(t.TempDir(), meta)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(paths.RunDir, "20260827T123000Z-test-machine-1234567890ab-smoke") {
		t.Fatalf("run dir = %q", paths.RunDir)
	}
	for _, path := range []string{paths.BenchDir, paths.StressDir, paths.SnapshotDir, paths.ServerInfoPath} {
		if path == "" {
			t.Fatalf("empty artifact path: %+v", paths)
		}
	}
}

func TestFetchServerInfoWritesPrettyJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/info" {
			t.Fatalf("path = %q, want /v1/info", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"instance":{"role":"master","go_version":"go1.27.0"}}`))
	}))
	defer server.Close()

	output := filepath.Join(t.TempDir(), "server-info.json")
	if err := fetchServerInfo(context.Background(), server.URL+"/v1/info", output); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); !strings.Contains(got, "\n  \"instance\": {") {
		t.Fatalf("server info was not formatted: %q", got)
	}
}

func TestFetchServerInfoRejectsInvalidJSON(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`not-json`))
	}))
	defer server.Close()

	err := fetchServerInfo(context.Background(), server.URL, filepath.Join(t.TempDir(), "server-info.json"))
	if err == nil {
		t.Fatal("expected invalid JSON error")
	}
}

func TestSanitizeName(t *testing.T) {
	if got := sanitizeName("Roman's MacBook Pro"); got != "roman-s-macbook-pro" {
		t.Fatalf("sanitize = %q", got)
	}
	if got := sanitizeName("  "); got != "machine" {
		t.Fatalf("empty sanitize = %q", got)
	}
}
