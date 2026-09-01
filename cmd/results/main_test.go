package main

import (
	"bytes"
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
	}, paths, "")

	if len(commands) != 4 {
		t.Fatalf("commands = %d, want 4", len(commands))
	}

	bench := commands[0]
	if bench.Name != "bench-smoke" || bench.OutputFile == "" {
		t.Fatalf("unexpected bench command: %+v", bench)
	}
	joined := strings.Join(bench.Command, " ")
	if strings.Contains(joined, "-address") {
		t.Fatalf("bench command should let the profile address win when -address is not explicit: %q", joined)
	}
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

func TestBuildCommandsCanOverrideProfileAddress(t *testing.T) {
	commands := buildCommands(config{
		mode:              modeSmoke,
		address:           "db.example:1945",
		addressOverride:   true,
		includeBenchmarks: true,
		includeStress:     false,
	}, artifacts{BenchDir: "benchmarks"}, "")

	joined := strings.Join(commands[0].Command, " ")
	if !strings.Contains(joined, "-address db.example:1945") {
		t.Fatalf("bench command %q does not contain explicit address override", joined)
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
	}, "")

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

func TestParseFlagsTracksExplicitAddress(t *testing.T) {
	defaulted := parseFlags([]string{"-mode", modeRelease})
	if defaulted.addressOverride {
		t.Fatal("default address should not override benchmark profiles")
	}

	explicit := parseFlags([]string{"-mode", modeRelease, "-address", "db.example:1945"})
	if !explicit.addressOverride {
		t.Fatal("explicit address should override benchmark profiles")
	}
	if explicit.address != "db.example:1945" {
		t.Fatalf("address = %q", explicit.address)
	}
}

func TestValidateBenchmarkProfileAddressesRejectsMissingAddressWithoutOverride(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing-address.yml")
	if err := os.WriteFile(missing, []byte("connections: 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	err := validateProfileListAddresses(config{includeBenchmarks: true}, []string{missing})
	if err == nil {
		t.Fatal("expected missing address error")
	}
	if !strings.Contains(err.Error(), "missing address") {
		t.Fatalf("error = %v", err)
	}

	err = validateProfileListAddresses(config{includeBenchmarks: true, addressOverride: true}, []string{missing})
	if err != nil {
		t.Fatalf("override should skip profile address validation: %v", err)
	}
}

func TestBuildCommandsPassesBenchAuthThroughPrivateEnv(t *testing.T) {
	commands := buildCommands(config{
		mode:              modeSmoke,
		address:           ":1945",
		includeBenchmarks: true,
		includeStress:     false,
	}, artifacts{BenchDir: "benchmarks"}, "secret-token-value")

	if len(commands) != 1 {
		t.Fatalf("commands = %d, want 1", len(commands))
	}
	command := commands[0]
	if strings.Contains(strings.Join(command.Command, " "), "secret-token-value") {
		t.Fatalf("command leaked token: %+v", command.Command)
	}
	if got := command.privateEnv["FQ_TOKEN"]; got != "secret-token-value" {
		t.Fatalf("private env token = %q", got)
	}
	if len(command.Env) != 1 || command.Env[0] != "FQ_TOKEN" {
		t.Fatalf("public env keys = %+v", command.Env)
	}
}

func TestBuildCommandsPassesBenchTLSFlags(t *testing.T) {
	commands := buildCommands(config{
		mode:              modeSmoke,
		address:           ":1945",
		includeBenchmarks: true,
		includeStress:     false,
		tlsCA:             "ca.crt",
		tlsCert:           "client.crt",
		tlsKey:            "client.key",
		tlsServerName:     "fq.internal",
		tlsSkipVerify:     true,
	}, artifacts{BenchDir: "benchmarks"}, "")

	joined := strings.Join(commands[0].Command, " ")
	for _, want := range []string{
		"-tls_ca ca.crt",
		"-tls_cert client.crt",
		"-tls_key client.key",
		"-tls_server_name fq.internal",
		"-tls_skip_verify",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("bench command %q does not contain %q", joined, want)
		}
	}
}

func TestResolveBenchTokenSources(t *testing.T) {
	t.Setenv("FQ_RESULTS_TOKEN", " env-token \n")
	fromEnv, err := resolveBenchToken(config{tokenEnv: "FQ_RESULTS_TOKEN"})
	if err != nil {
		t.Fatal(err)
	}
	if fromEnv != "env-token" {
		t.Fatalf("env token = %q", fromEnv)
	}

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte(" file-token \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fromFile, err := resolveBenchToken(config{tokenFile: tokenFile})
	if err != nil {
		t.Fatal(err)
	}
	if fromFile != "file-token" {
		t.Fatalf("file token = %q", fromFile)
	}

	_, err = resolveBenchToken(config{token: "one", tokenEnv: "FQ_RESULTS_TOKEN"})
	if err == nil {
		t.Fatal("expected multiple token sources error")
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

func TestReleaseProgressOutput(t *testing.T) {
	var out bytes.Buffer
	commands := []runCommand{
		{Name: "bench-smoke", Kind: "benchmark"},
		{Name: "stress-crash-loop", Kind: "stress"},
	}

	printReleaseStart(&out, artifacts{RunDir: "runs/release"}, commands)
	printCommandStart(&out, 1, 2, commands[0], "bench-smoke.log")
	printCommandFinish(&out, runResult{
		Name:     "bench-smoke",
		Started:  time.Unix(0, 0),
		Finished: time.Unix(0, int64(1500*time.Millisecond)),
	}, commands[0])
	printCommandFinish(&out, runResult{
		Name:     "stress-crash-loop",
		Started:  time.Unix(0, 0),
		Finished: time.Unix(2, 0),
		Error:    "exit status 1",
	}, commands[1])

	text := out.String()
	for _, want := range []string{
		"release run confirmed",
		"results run directory: runs/release",
		"commands planned: 2",
		"1/2 bench-smoke [benchmark]",
		"starting 1/2 bench-smoke [benchmark], log: bench-smoke.log",
		"finished bench-smoke in 1.5s",
		"failed stress-crash-loop after 2s: exit status 1",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("progress output %q does not contain %q", text, want)
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
