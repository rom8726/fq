package main

import (
	"encoding/json"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"
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
	cfg := benchConfig{outputFormat: "json", outputFile: filepath.Join(t.TempDir(), "report.json")}
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
