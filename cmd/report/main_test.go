package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunWritesMarkdownReport(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	inputDir := filepath.Join(dir, "results")
	require.NoError(t, os.MkdirAll(inputDir, 0o750))

	started := time.Date(2026, 8, 28, 6, 0, 0, 0, time.UTC)
	writeFixture(t, inputDir, "release-sw-uniform.json", benchmarkReport{
		Metadata: reportMetadata{
			StartedAt:       started,
			FinishedAt:      started.Add(time.Minute),
			GoVersion:       "go1.27.0",
			GOOS:            "linux",
			GOARCH:          "amd64",
			NumCPU:          4,
			ConfigHash:      "abc",
			Profile:         "./benchmarks/profiles/release-sw-uniform.yml",
			Address:         "10.0.0.1:1945",
			Connections:     100,
			Warmup:          "5s",
			Duration:        "1m0s",
			RequestTimeout:  "5s",
			IdleTimeout:     "1m0s",
			MaxMessageSize:  4096,
			QueryTemplate:   "RLIMIT SW {key} 10000 {batch}",
			KeyDistribution: "uniform",
			KeyRange:        100000,
			BatchSize:       10,
			Seed:            42,
		},
		Summary: reportSummary{
			MeasuredDurationSeconds: 60,
			Requests:                4_400_000,
			Errors:                  4,
			ErrorRate:               0.000001,
			ThroughputRPS:           73_000,
			Latency: latencySummary{
				P50Micros: 1000,
				P95Micros: 3000,
				P99Micros: 5000,
			},
		},
	})
	writeFixture(t, inputDir, "release-sw-zipfian.json", benchmarkReport{
		Metadata: reportMetadata{
			StartedAt:       started,
			FinishedAt:      started.Add(time.Minute),
			GoVersion:       "go1.27.0",
			GOOS:            "linux",
			GOARCH:          "amd64",
			NumCPU:          4,
			ConfigHash:      "def",
			Profile:         "./benchmarks/profiles/release-sw-zipfian.yml",
			Address:         "10.0.0.1:1945",
			Connections:     100,
			Warmup:          "5s",
			Duration:        "1m0s",
			RequestTimeout:  "5s",
			IdleTimeout:     "1m0s",
			MaxMessageSize:  4096,
			QueryTemplate:   "RLIMIT SW {key} 10000 {batch}",
			KeyDistribution: "zipfian",
			KeyRange:        100000,
			BatchSize:       10,
			Seed:            42,
		},
		Summary: reportSummary{
			MeasuredDurationSeconds: 60,
			Requests:                3_600_000,
			Errors:                  4,
			ErrorRate:               0.000001,
			ThroughputRPS:           60_000,
			Latency: latencySummary{
				P50Micros: 1200,
				P95Micros: 4000,
				P99Micros: 7000,
			},
		},
	})

	output := filepath.Join(dir, "report.md")
	err := run([]string{
		"-input", inputDir,
		"-output", output,
		"-server_machine", "db-4cpu",
		"-client_machine", "bench-8cpu",
		"-client_cpu", "8",
	})
	require.NoError(t, err)

	data, err := os.ReadFile(output)
	require.NoError(t, err)
	report := string(data)

	require.Contains(t, report, "# FQ Benchmark Report")
	require.Contains(t, report, "| Database CPU | 4 |")
	require.Contains(t, report, "| Benchmark client CPU | 8 |")
	require.Contains(t, report, "| `release-sw-uniform` | uniform | 100000 | 10 | 73000.0 |")
	require.Contains(t, report, "Sliding window with Zipfian keys reached")
	require.NotContains(t, report, "10.0.0.1:1945")
}

func TestDefaultOutputPathUsesCurrentDate(t *testing.T) {
	path := defaultOutputPath(time.Date(2026, 9, 1, 12, 30, 0, 0, time.UTC))

	require.Equal(t, filepath.Join("benchmarks", "reports", "report_2026_09_01.md"), path)
}

func TestRunDirectoryReportIncludesPublishableArtifacts(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	runDir := filepath.Join(dir, "runs", "20260901T120000Z-bench-host-abc123-release")
	benchDir := filepath.Join(runDir, "benchmarks")
	stressDir := filepath.Join(runDir, "stress")
	require.NoError(t, os.MkdirAll(benchDir, 0o750))
	require.NoError(t, os.MkdirAll(stressDir, 0o750))

	started := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	writeFixture(t, benchDir, "release-hot-counter.json", benchmarkReport{
		Metadata: reportMetadata{
			StartedAt:       started,
			FinishedAt:      started.Add(time.Minute),
			GoVersion:       "go1.27.0",
			GOOS:            "linux",
			GOARCH:          "amd64",
			NumCPU:          8,
			ConfigHash:      "bench-hash",
			Profile:         "./benchmarks/profiles/release-hot-counter.yml",
			Connections:     100,
			Warmup:          "5s",
			Duration:        "1m0s",
			RequestTimeout:  "5s",
			IdleTimeout:     "1m0s",
			MaxMessageSize:  4096,
			QueryTemplate:   "INCR {key} {batch}",
			KeyDistribution: "sequential",
			KeyRange:        1,
			BatchSize:       600,
			Seed:            42,
		},
		Summary: reportSummary{
			MeasuredDurationSeconds: 60,
			Requests:                6_000_000,
			ThroughputRPS:           100_000,
			Latency: latencySummary{
				P50Micros: 900,
				P95Micros: 1900,
				P99Micros: 2900,
			},
		},
	})
	writeJSONFixture(t, runDir, "metadata.json", resultsMetadata{
		Mode:      "release",
		GitCommit: "abcdef1234567890",
		GitDirty:  false,
		Hostname:  "bench-host",
		Machine:   "dedicated-bench",
		GOOS:      "linux",
		GOARCH:    "amd64",
		GoVersion: "go1.27.0",
		NumCPU:    8,
		ConfigSHA256: map[string]string{
			"config.yml": "config-hash",
		},
		GeneratedAt: started,
	})
	writeJSONFixture(t, runDir, "manifest.json", resultsManifest{
		Metadata: resultsMetadata{Mode: "release"},
		Artifacts: resultsArtifacts{
			ServerInfoPath: filepath.Join(runDir, "server-info.json"),
		},
		Commands: []resultsCommand{
			{
				Name: "bench-release-hot-counter",
				Kind: "benchmark",
				Command: []string{
					"go", "run", "./cmd/bench",
					"-profile", "benchmarks/profiles/release-hot-counter.yml",
					"-address", "10.0.0.2:1945",
				},
				OutputFile: filepath.Join(runDir, "benchmarks", "release-hot-counter.json"),
			},
			{
				Name:       "stress-crash-loop",
				Kind:       "stress",
				Command:    []string{"go", "run", "./cmd/stress", "-scenario", "crash-loop"},
				OutputFile: filepath.Join(runDir, "stress", "crash-loop.json"),
			},
			{
				Name:       "stress-dump-recovery",
				Kind:       "stress",
				Command:    []string{"go", "run", "./cmd/stress", "-scenario", "dump-recovery"},
				OutputFile: filepath.Join(runDir, "stress", "dump-recovery.json"),
			},
		},
		Results: []resultsCommandResult{
			{Name: "bench-release-hot-counter", ExitCode: 0},
			{Name: "stress-crash-loop", ExitCode: 0},
			{Name: "stress-dump-recovery", ExitCode: 1},
		},
	})
	syncCommit := "on"
	writeJSONFixture(t, runDir, "server-info.json", serverInfoReport{
		Instance: &serverInstanceInfo{
			Version:    "1.2.3",
			Commit:     "dbcommit123",
			Hostname:   "db-host",
			NumCPU:     32,
			GoVersion:  "go1.27.1",
			Platform:   "linux/arm64",
			Role:       "master",
			ListenAddr: "10.0.0.2:1945",
		},
		Persistence: &serverPersistenceInfo{
			Mode:       "wal_and_dump",
			SyncCommit: &syncCommit,
		},
		Engine: &serverEngineInfo{
			Partitions: 16,
		},
		Repl: &serverReplInfo{
			Role:            "master",
			ProtocolVersion: 1,
		},
	})
	writeJSONFixture(t, stressDir, "crash-loop.json", stressReport{
		Scenario:       "crash-loop",
		Status:         "passed",
		DurationMillis: 30_000,
		Result: stressResult{
			Operations:      1234,
			Restarts:        14,
			TransientErrors: 2,
		},
	})

	output := filepath.Join(dir, "published.md")
	err := run([]string{"-input_dir", runDir, "-output_file", output})
	require.NoError(t, err)

	data, err := os.ReadFile(output)
	require.NoError(t, err)
	report := string(data)

	require.Contains(t, report, "## Release Run Metadata")
	require.Contains(t, report, "| Git commit | `abcdef1234567890` |")
	require.Contains(t, report, "| Config SHA-256 `config.yml` | `config-hash` |")
	require.Contains(t, report, "| Database version | `1.2.3` |")
	require.Contains(t, report, "| Database commit | `dbcommit123` |")
	require.Contains(t, report, "| Database replication role | `master` |")
	require.Contains(t, report, "| Database server | remote Linux server |")
	require.Contains(t, report, "| Database CPU | 32 |")
	require.Contains(t, report, "| Database partitions | 16 |")
	require.Contains(t, report, "| Database OS / Arch | linux/arm64 |")
	require.Contains(t, report, "| Benchmark client | remote Linux benchmark client |")
	require.Contains(t, report, "| Benchmark client CPU | 8 |")
	require.Contains(t, report, "| Persistence | wal_and_dump, sync_commit=on |")
	require.Contains(t, report, "## Command Manifest")
	require.Contains(t, report, "-address <redacted>")
	require.Contains(t, report, "| `stress-crash-loop` | stress |")
	require.Contains(t, report, "| `stress-dump-recovery` | stress |")
	require.Contains(t, report, "failed: exit code 1")
	require.Contains(t, report, "## Stress Results")
	require.Contains(t, report, "| `crash-loop` | passed | 1234 | 14 | 0 | 2 | 30s |")
	require.NotContains(t, report, "bench-host")
	require.NotContains(t, report, "db-host")
	require.NotContains(t, report, "10.0.0.2")
	require.NotContains(t, report, runDir)
}

func writeFixture(t *testing.T, dir string, name string, report benchmarkReport) {
	t.Helper()

	writeJSONFixture(t, dir, name, report)
}

func writeJSONFixture(t *testing.T, dir string, name string, value interface{}) {
	t.Helper()

	data, err := jsonMarshalIndent(value)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0o644))
}

func jsonMarshalIndent(value interface{}) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, err
	}

	return append(data, '\n'), nil
}
