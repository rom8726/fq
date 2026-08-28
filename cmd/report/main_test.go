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

func writeFixture(t *testing.T, dir string, name string, report benchmarkReport) {
	t.Helper()

	data, err := jsonMarshalIndent(report)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), data, 0o644))
}

func jsonMarshalIndent(report benchmarkReport) ([]byte, error) {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, err
	}

	return append(data, '\n'), nil
}
