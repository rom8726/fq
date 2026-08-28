package main

import (
	"context"
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fq-db/fq/internal/network"
)

func TestMakeReportComputesMeasuredSummary(t *testing.T) {
	cfg := benchConfig{
		address:         ":1945",
		connections:     2,
		warmup:          time.Second,
		duration:        10 * time.Second,
		rps:             100,
		requestTimeout:  time.Second,
		idleTimeout:     time.Minute,
		maxMessageSize:  4096,
		queryTemplate:   defaultQueryTemplate,
		keyPrefix:       "bench",
		keyDistribution: "uniform",
		keyRange:        100,
		batchSize:       600,
		seed:            42,
	}
	startedAt := time.Unix(100, 0)
	finishedAt := startedAt.Add(11 * time.Second)
	report := makeReport(
		cfg,
		startedAt,
		finishedAt,
		10,
		1,
		[]time.Duration{
			time.Millisecond,
			2 * time.Millisecond,
			3 * time.Millisecond,
			4 * time.Millisecond,
			5 * time.Millisecond,
		},
		"last error",
	)

	if report.Summary.MeasuredDurationSeconds != 10 {
		t.Fatalf("measured duration = %f", report.Summary.MeasuredDurationSeconds)
	}
	if report.Summary.ThroughputRPS != 1 {
		t.Fatalf("throughput = %f", report.Summary.ThroughputRPS)
	}
	if report.Summary.ErrorRate != 0.1 {
		t.Fatalf("error rate = %f", report.Summary.ErrorRate)
	}
	if report.Summary.Latency.P50Micros != 3000 {
		t.Fatalf("p50 = %d", report.Summary.Latency.P50Micros)
	}
	if report.Summary.Latency.P999Micros != 5000 {
		t.Fatalf("p99.9 = %d", report.Summary.Latency.P999Micros)
	}
	if report.Metadata.ConfigHash == "" {
		t.Fatal("config hash is empty")
	}
	if report.Metadata.KeyDistribution != "uniform" {
		t.Fatalf("key distribution = %q", report.Metadata.KeyDistribution)
	}
}

func TestWriteJSONReportToFile(t *testing.T) {
	cfg := benchConfig{outputFormat: "json", outputFile: filepath.Join(t.TempDir(), "nested", "report.json")}
	report := runReport{
		Metadata: reportMetadata{ConfigHash: "abc"},
		Summary:  reportSummary{Requests: 7},
	}

	if err := writeReport(cfg, report); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(cfg.outputFile)
	if err != nil {
		t.Fatal(err)
	}

	var decoded runReport
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Summary.Requests != 7 {
		t.Fatalf("requests = %d", decoded.Summary.Requests)
	}
}

func TestRecordResultKeepsWarmupErrorForDiagnostics(t *testing.T) {
	var measuredCount uint64
	var measuredErrors uint64
	var windowCount int
	var windowErrors int
	var windowLatencies []time.Duration
	var measuredLatencies []time.Duration
	var lastError string

	recordResult(
		result{err: os.ErrDeadlineExceeded, errText: "connect: refused"},
		false,
		&measuredCount,
		&measuredErrors,
		&windowCount,
		&windowErrors,
		&windowLatencies,
		&measuredLatencies,
		&lastError,
	)

	if measuredCount != 0 || measuredErrors != 0 || windowCount != 0 || windowErrors != 0 {
		t.Fatalf("warmup result was counted: measured=%d/%d window=%d/%d", measuredCount, measuredErrors, windowCount, windowErrors)
	}
	if lastError != "connect: refused" {
		t.Fatalf("last error = %q", lastError)
	}
}

func TestRecordResultIgnoresShutdownErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "context_canceled", err: context.Canceled},
		{name: "idle_timeout", err: network.ErrIdleTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var measuredCount uint64
			var measuredErrors uint64
			var windowCount int
			var windowErrors int
			var windowLatencies []time.Duration
			var measuredLatencies []time.Duration
			lastError := "previous"

			recordResult(
				result{err: tc.err, errText: tc.err.Error(), latency: time.Second},
				true,
				&measuredCount,
				&measuredErrors,
				&windowCount,
				&windowErrors,
				&windowLatencies,
				&measuredLatencies,
				&lastError,
			)

			if measuredCount != 0 || measuredErrors != 0 || windowCount != 0 || windowErrors != 0 {
				t.Fatalf("ignored result was counted: measured=%d/%d window=%d/%d",
					measuredCount,
					measuredErrors,
					windowCount,
					windowErrors,
				)
			}
			if len(windowLatencies) != 0 || len(measuredLatencies) != 0 {
				t.Fatalf("ignored result latency was recorded: window=%d measured=%d",
					len(windowLatencies),
					len(measuredLatencies),
				)
			}
			if lastError != "previous" {
				t.Fatalf("last error = %q", lastError)
			}
		})
	}
}

func TestParseArgsLoadsProfile(t *testing.T) {
	profilePath := filepath.Join(t.TempDir(), "profile.yml")
	err := os.WriteFile(profilePath, []byte(`
address: ":2000"
connections: 12
warmup: 3s
duration: 15s
rps: 750
request_timeout: 2s
idle_timeout: 20s
max_message_size: 8KB
query: "RLIMIT FW {key} 100 {batch}"
key_prefix: prof
key_distribution: zipfian
key_start: 10
key_range: 1000
batch: 7
output: json
output_file: benchmarks/results/profile.json
seed: 99
`), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := parseArgs([]string{"-profile", profilePath})
	if err != nil {
		t.Fatal(err)
	}

	if cfg.profilePath != profilePath {
		t.Fatalf("profile path = %q", cfg.profilePath)
	}
	if cfg.address != ":2000" || cfg.connections != 12 || cfg.warmup != 3*time.Second || cfg.duration != 15*time.Second {
		t.Fatalf("profile was not loaded: %+v", cfg)
	}
	if cfg.maxMessageSize != 8<<10 {
		t.Fatalf("max message size = %d", cfg.maxMessageSize)
	}
	if cfg.queryTemplate != "RLIMIT FW {key} 100 {batch}" || cfg.keyDistribution != zipfian || cfg.batchSize != 7 {
		t.Fatalf("workload fields were not loaded: %+v", cfg)
	}
	if cfg.outputFormat != "json" || cfg.outputFile != "benchmarks/results/profile.json" || cfg.seed != 99 {
		t.Fatalf("output fields were not loaded: %+v", cfg)
	}
}

func TestParseArgsLetsFlagsOverrideProfile(t *testing.T) {
	profilePath := filepath.Join(t.TempDir(), "profile.yml")
	err := os.WriteFile(profilePath, []byte(`
connections: 12
duration: 15s
key_range: 1000
output: json
`), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := parseArgs([]string{
		"-profile", profilePath,
		"-connections", "3",
		"-duration", "1s",
		"-key_range", "55",
		"-output", "csv",
	})
	if err != nil {
		t.Fatal(err)
	}

	if cfg.connections != 3 {
		t.Fatalf("connections = %d", cfg.connections)
	}
	if cfg.duration != time.Second {
		t.Fatalf("duration = %s", cfg.duration)
	}
	if cfg.keyRange != 55 {
		t.Fatalf("key range = %d", cfg.keyRange)
	}
	if cfg.outputFormat != "csv" {
		t.Fatalf("output = %s", cfg.outputFormat)
	}
}

func TestParseArgsRejectsUnknownProfileFields(t *testing.T) {
	profilePath := filepath.Join(t.TempDir(), "profile.yml")
	err := os.WriteFile(profilePath, []byte("key_distrubution: uniform\n"), 0o644)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := parseArgs([]string{"-profile", profilePath}); err == nil {
		t.Fatal("expected unknown profile field error")
	}
}

func TestRepositoryProfilesParse(t *testing.T) {
	profiles, err := filepath.Glob(filepath.Join("..", "..", "benchmarks", "profiles", "*.yml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(profiles) == 0 {
		t.Fatal("no benchmark profiles found")
	}

	for _, profile := range profiles {
		t.Run(filepath.Base(profile), func(t *testing.T) {
			if _, err := parseArgs([]string{"-profile", profile}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNextKeyOffsetDistributions(t *testing.T) {
	sequential := benchConfig{connections: 4, keyRange: 10, keyDistribution: "sequential"}
	if got := nextKeyOffset(sequential, 2, 3, rand.New(rand.NewSource(1)), nil); got != 4 {
		t.Fatalf("sequential offset = %d", got)
	}

	uniform := benchConfig{keyRange: 10, keyDistribution: "uniform"}
	first := nextKeyOffset(uniform, 0, 0, rand.New(rand.NewSource(7)), nil)
	second := nextKeyOffset(uniform, 0, 0, rand.New(rand.NewSource(7)), nil)
	if first != second {
		t.Fatalf("uniform seed is not deterministic: %d != %d", first, second)
	}
	if first >= uniform.keyRange {
		t.Fatalf("uniform offset out of range: %d", first)
	}

	zipfian := benchConfig{keyRange: 1, keyDistribution: "zipfian"}
	if got := nextKeyOffset(zipfian, 0, 0, rand.New(rand.NewSource(1)), nil); got != 0 {
		t.Fatalf("single-key zipfian offset = %d", got)
	}
}
