package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	defaultInputDir     = "benchmarks/results"
	defaultOutputFile   = "benchmarks/results/report.md"
	defaultClientCPU    = 8
	highErrorRate       = 0.01
	percentMultiplier   = 100
	minComparableResult = 2
)

type config struct {
	inputDir      string
	outputFile    string
	title         string
	serverMachine string
	clientMachine string
	clientCPU     int
	persistence   string
	notes         string
}

type benchmarkReport struct {
	Metadata reportMetadata `json:"metadata"`
	Summary  reportSummary  `json:"summary"`
	source   string
}

type reportMetadata struct {
	StartedAt       time.Time `json:"started_at"`
	FinishedAt      time.Time `json:"finished_at"`
	GoVersion       string    `json:"go_version"`
	GOOS            string    `json:"goos"`
	GOARCH          string    `json:"goarch"`
	NumCPU          int       `json:"num_cpu"`
	ConfigHash      string    `json:"config_hash"`
	Profile         string    `json:"profile,omitempty"`
	Address         string    `json:"address"`
	Connections     int       `json:"connections"`
	Warmup          string    `json:"warmup"`
	Duration        string    `json:"duration"`
	TargetRPS       float64   `json:"target_rps"`
	RequestTimeout  string    `json:"request_timeout"`
	IdleTimeout     string    `json:"idle_timeout"`
	MaxMessageSize  int       `json:"max_message_size"`
	QueryTemplate   string    `json:"query_template"`
	KeyPrefix       string    `json:"key_prefix"`
	KeyDistribution string    `json:"key_distribution"`
	KeyStart        uint64    `json:"key_start"`
	KeyRange        uint64    `json:"key_range"`
	BatchSize       uint64    `json:"batch_size"`
	Seed            int64     `json:"seed"`
}

type reportSummary struct {
	MeasuredDurationSeconds float64        `json:"measured_duration_seconds"`
	Requests                uint64         `json:"requests"`
	Errors                  uint64         `json:"errors"`
	ErrorRate               float64        `json:"error_rate"`
	ThroughputRPS           float64        `json:"throughput_rps"`
	Latency                 latencySummary `json:"latency"`
	LastError               string         `json:"last_error,omitempty"`
}

type latencySummary struct {
	P50Micros  int64 `json:"p50_micros"`
	P95Micros  int64 `json:"p95_micros"`
	P99Micros  int64 `json:"p99_micros"`
	P999Micros int64 `json:"p999_micros"`
	MaxMicros  int64 `json:"max_micros"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "report failed:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg := parseFlags(args)
	reports, err := loadReports(cfg.inputDir)
	if err != nil {
		return err
	}
	if len(reports) == 0 {
		return fmt.Errorf("no benchmark JSON reports found in %s", cfg.inputDir)
	}

	sortReports(reports)
	report := renderMarkdown(cfg, reports)
	if cfg.outputFile == "-" {
		fmt.Print(report)

		return nil
	}

	if err := os.MkdirAll(filepath.Dir(cfg.outputFile), 0o750); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	if err := os.WriteFile(cfg.outputFile, []byte(report), 0o644); err != nil {
		return fmt.Errorf("write markdown report: %w", err)
	}

	fmt.Printf("Report written to %s\n", cfg.outputFile)

	return nil
}

func parseFlags(args []string) config {
	cfg := config{}
	flags := flag.NewFlagSet("report", flag.ExitOnError)
	flags.StringVar(&cfg.inputDir, "input", defaultInputDir, "directory with benchmark JSON reports")
	flags.StringVar(&cfg.outputFile, "output", defaultOutputFile, "markdown report output path; use - for stdout")
	flags.StringVar(&cfg.title, "title", "FQ Benchmark Report", "markdown report title")
	flags.StringVar(&cfg.serverMachine, "server_machine", "remote Linux server", "database server label")
	flags.StringVar(&cfg.clientMachine, "client_machine", "remote Linux benchmark client", "benchmark client label")
	flags.IntVar(&cfg.clientCPU, "client_cpu", defaultClientCPU, "benchmark client CPU count")
	flags.StringVar(&cfg.persistence, "persistence", "WAL + dump, sync_commit=off", "persistence mode description")
	flags.StringVar(&cfg.notes, "notes", "", "extra markdown sentence added to the methodology section")
	_ = flags.Parse(args)

	return cfg
}

func loadReports(inputDir string) ([]benchmarkReport, error) {
	entries, err := os.ReadDir(inputDir)
	if err != nil {
		return nil, fmt.Errorf("read input directory: %w", err)
	}

	reports := make([]benchmarkReport, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		path := filepath.Join(inputDir, entry.Name())
		report, err := loadReport(path)
		if err != nil {
			return nil, err
		}
		reports = append(reports, report)
	}

	return reports, nil
}

func loadReport(path string) (benchmarkReport, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return benchmarkReport{}, fmt.Errorf("read %s: %w", path, err)
	}

	var report benchmarkReport
	if err := json.Unmarshal(data, &report); err != nil {
		return benchmarkReport{}, fmt.Errorf("parse %s: %w", path, err)
	}
	if report.Metadata.QueryTemplate == "" {
		return benchmarkReport{}, fmt.Errorf("parse %s: missing metadata.query_template", path)
	}
	report.source = path

	return report, nil
}

func sortReports(reports []benchmarkReport) {
	sort.Slice(reports, func(i, j int) bool {
		left := reports[i]
		right := reports[j]
		if left.Summary.ThroughputRPS != right.Summary.ThroughputRPS {
			return left.Summary.ThroughputRPS > right.Summary.ThroughputRPS
		}

		return left.source < right.source
	})
}

func renderMarkdown(cfg config, reports []benchmarkReport) string {
	var b strings.Builder
	now := time.Now().UTC().Format(time.RFC3339)
	successful := filterReports(reports, func(report *benchmarkReport) bool {
		return report.Summary.ErrorRate < highErrorRate
	})

	fmt.Fprintf(&b, "# %s\n\n", cfg.title)
	fmt.Fprintf(&b, "_Generated at %s from `%s`._\n\n", now, slashPath(cfg.inputDir))
	fmt.Fprintf(&b, "## Summary\n\n")
	if len(successful) > 0 {
		best := successful[0]
		fmt.Fprintf(
			&b,
			"The benchmark suite contains %d runs. "+
				"The fastest success-heavy workload was `%s` at **%s rps** with p99 latency **%s**.\n",
			len(reports),
			scenarioName(best),
			formatFloat(best.Summary.ThroughputRPS),
			formatMicros(best.Summary.Latency.P99Micros),
		)
	} else {
		fmt.Fprintf(&b, "The benchmark suite contains %d runs. No success-heavy runs were found.\n", len(reports))
	}
	if reports[0].Summary.ErrorRate >= highErrorRate {
		fmt.Fprintf(
			&b,
			"The highest raw throughput was `%s` at **%s rps**, "+
				"but its error rate was **%s**, so it is treated as an error-path run.\n",
			scenarioName(&reports[0]),
			formatFloat(reports[0].Summary.ThroughputRPS),
			formatPercent(reports[0].Summary.ErrorRate),
		)
	}
	fmt.Fprintln(&b)
	renderObservations(&b, reports)
	renderEnvironment(&b, cfg, reports)
	renderMethodology(&b, cfg, reports)
	renderHeadlineTable(&b, reports)
	renderWorkloadTable(&b, reports)
	renderLatencyTable(&b, reports)
	renderComparisonTable(&b, reports)
	renderNotes(&b, reports)

	return b.String()
}

func renderObservations(b *strings.Builder, reports []benchmarkReport) {
	fmt.Fprintln(b, "## Observations")
	fmt.Fprintln(b)

	successful := filterReports(reports, func(report *benchmarkReport) bool {
		return report.Summary.ErrorRate < highErrorRate
	})
	if len(successful) > 0 {
		best := successful[0]
		fmt.Fprintf(
			b,
			"- Best success-heavy workload: `%s` at **%s rps**, p50 **%s**, p99 **%s**.\n",
			scenarioName(best),
			formatFloat(best.Summary.ThroughputRPS),
			formatMicros(best.Summary.Latency.P50Micros),
			formatMicros(best.Summary.Latency.P99Micros),
		)
	}

	if swUniform, ok := findScenario(reports, "RLIMIT SW", "uniform"); ok {
		if swZipfian, ok := findScenario(reports, "RLIMIT SW", "zipfian"); ok {
			drop := throughputDrop(swUniform.Summary.ThroughputRPS, swZipfian.Summary.ThroughputRPS)
			fmt.Fprintf(
				b,
				"- Sliding window with Zipfian keys reached **%s rps** versus **%s rps** "+
					"with uniform keys; skew reduced throughput by **%s**.\n",
				formatFloat(swZipfian.Summary.ThroughputRPS),
				formatFloat(swUniform.Summary.ThroughputRPS),
				formatPercent(drop),
			)
		}
	}

	if counterUniform, ok := findScenario(reports, "INCR", "uniform"); ok {
		if counterHot, ok := findScenario(reports, "INCR", "sequential"); ok && counterHot.Metadata.KeyRange == 1 {
			ratio := ratio(counterHot.Summary.ThroughputRPS, counterUniform.Summary.ThroughputRPS)
			fmt.Fprintf(
				b,
				"- Single-key counter throughput was **%s rps**, or **%sx** of the uniform counter run.\n",
				formatFloat(counterHot.Summary.ThroughputRPS),
				formatRatio(ratio),
			)
		}
	}

	errorHeavy := filterReports(reports, func(report *benchmarkReport) bool {
		return report.Summary.ErrorRate >= highErrorRate
	})
	if len(errorHeavy) > 0 {
		names := make([]string, 0, len(errorHeavy))
		for i := range errorHeavy {
			names = append(names, "`"+scenarioName(errorHeavy[i])+"`")
		}
		fmt.Fprintf(
			b,
			"- %s had high error rates and should be interpreted as rejection/error-path throughput, "+
				"not a normal success-path benchmark.\n",
			strings.Join(names, ", "),
		)
	}

	fmt.Fprintln(b)
}

func renderEnvironment(b *strings.Builder, cfg config, reports []benchmarkReport) {
	first := reports[0]
	fmt.Fprintln(b, "## Environment")
	fmt.Fprintln(b)
	fmt.Fprintln(b, "| Component | Value |")
	fmt.Fprintln(b, "| --- | --- |")
	fmt.Fprintf(b, "| Database server | %s |\n", escapeCell(cfg.serverMachine))
	fmt.Fprintf(b, "| Database CPU | %d |\n", first.Metadata.NumCPU)
	fmt.Fprintf(b, "| Benchmark client | %s |\n", escapeCell(cfg.clientMachine))
	fmt.Fprintf(b, "| Benchmark client CPU | %d |\n", cfg.clientCPU)
	fmt.Fprintf(b, "| OS / Arch | %s/%s |\n", first.Metadata.GOOS, first.Metadata.GOARCH)
	fmt.Fprintf(b, "| Go version | %s |\n", first.Metadata.GoVersion)
	fmt.Fprintf(b, "| Persistence | %s |\n", escapeCell(cfg.persistence))
	fmt.Fprintln(b)
}

func renderMethodology(b *strings.Builder, cfg config, reports []benchmarkReport) {
	first := reports[0]
	fmt.Fprintln(b, "## Methodology")
	fmt.Fprintln(b)
	fmt.Fprintln(
		b,
		"Benchmarks were executed by a remote client against a dedicated FQ database server over TCP. "+
			"Each result excludes the warmup interval and reports measured request throughput, "+
			"transport/protocol errors, and end-to-end request latency percentiles observed by the client.",
	)
	fmt.Fprintln(b)
	fmt.Fprintf(
		b,
		"Unless stated otherwise, runs used `%d` connections, `%s` warmup, `%s` measured duration, "+
			"`%s` request timeout, `%s` idle timeout, and max message size `%d` bytes.\n\n",
		first.Metadata.Connections,
		first.Metadata.Warmup,
		first.Metadata.Duration,
		first.Metadata.RequestTimeout,
		first.Metadata.IdleTimeout,
		first.Metadata.MaxMessageSize,
	)
	if cfg.notes != "" {
		fmt.Fprintln(b, cfg.notes)
		fmt.Fprintln(b)
	}
}

func renderHeadlineTable(b *strings.Builder, reports []benchmarkReport) {
	fmt.Fprintln(b, "## Results")
	fmt.Fprintln(b)
	fmt.Fprintln(
		b,
		"| Scenario | Distribution | Keys | Batch | RPS | Errors | Error rate | p50 | p95 | p99 | p99.9 | Max |",
	)
	fmt.Fprintln(
		b,
		"| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
	)
	for i := range reports {
		report := &reports[i]
		fmt.Fprintf(
			b,
			"| `%s` | %s | %s | %d | %s | %d | %s | %s | %s | %s | %s | %s |\n",
			scenarioName(report),
			report.Metadata.KeyDistribution,
			formatUint(report.Metadata.KeyRange),
			report.Metadata.BatchSize,
			formatFloat(report.Summary.ThroughputRPS),
			report.Summary.Errors,
			formatPercent(report.Summary.ErrorRate),
			formatMicros(report.Summary.Latency.P50Micros),
			formatMicros(report.Summary.Latency.P95Micros),
			formatMicros(report.Summary.Latency.P99Micros),
			formatMicros(report.Summary.Latency.P999Micros),
			formatMicros(report.Summary.Latency.MaxMicros),
		)
	}
	fmt.Fprintln(b)
}

func renderWorkloadTable(b *strings.Builder, reports []benchmarkReport) {
	fmt.Fprintln(b, "## Workloads")
	fmt.Fprintln(b)
	fmt.Fprintln(b, "| Source | Query | Connections | Target RPS | Duration | Seed | Config hash |")
	fmt.Fprintln(b, "| --- | --- | ---: | ---: | --- | ---: | --- |")
	for i := range reports {
		report := &reports[i]
		fmt.Fprintf(
			b,
			"| `%s` | `%s` | %d | %s | %s | %d | `%s` |\n",
			slashPath(report.source),
			report.Metadata.QueryTemplate,
			report.Metadata.Connections,
			formatTargetRPS(report.Metadata.TargetRPS),
			report.Metadata.Duration,
			report.Metadata.Seed,
			report.Metadata.ConfigHash,
		)
	}
	fmt.Fprintln(b)
}

func renderLatencyTable(b *strings.Builder, reports []benchmarkReport) {
	fmt.Fprintln(b, "## Latency Details")
	fmt.Fprintln(b)
	fmt.Fprintln(b, "| Scenario | Requests | Measured seconds | p50 us | p95 us | p99 us | p99.9 us | Max us |")
	fmt.Fprintln(b, "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
	for i := range reports {
		report := &reports[i]
		fmt.Fprintf(
			b,
			"| `%s` | %s | %s | %d | %d | %d | %d | %d |\n",
			scenarioName(report),
			formatUint(report.Summary.Requests),
			formatFloat(report.Summary.MeasuredDurationSeconds),
			report.Summary.Latency.P50Micros,
			report.Summary.Latency.P95Micros,
			report.Summary.Latency.P99Micros,
			report.Summary.Latency.P999Micros,
			report.Summary.Latency.MaxMicros,
		)
	}
	fmt.Fprintln(b)
}

func renderComparisonTable(b *strings.Builder, reports []benchmarkReport) {
	successful := filterReports(reports, func(report *benchmarkReport) bool {
		return report.Summary.ErrorRate < highErrorRate
	})
	if len(successful) < minComparableResult {
		return
	}

	best := successful[0]
	fmt.Fprintln(b, "## Relative Throughput")
	fmt.Fprintln(b)
	fmt.Fprintf(b, "Relative throughput is normalized to `%s`.\n\n", scenarioName(best))
	fmt.Fprintln(b, "| Scenario | RPS | Relative |")
	fmt.Fprintln(b, "| --- | ---: | ---: |")
	for i := range successful {
		report := successful[i]
		fmt.Fprintf(
			b,
			"| `%s` | %s | %sx |\n",
			scenarioName(report),
			formatFloat(report.Summary.ThroughputRPS),
			formatRatio(ratio(report.Summary.ThroughputRPS, best.Summary.ThroughputRPS)),
		)
	}
	fmt.Fprintln(b)
}

func renderNotes(b *strings.Builder, reports []benchmarkReport) {
	fmt.Fprintln(b, "## Interpretation Notes")
	fmt.Fprintln(b)
	fmt.Fprintln(b, "- `metadata.num_cpu` is treated as the database server CPU count for this report.")
	fmt.Fprintln(
		b,
		"- `last_error` is intentionally omitted from the tables because graceful benchmark shutdown "+
			"can leave a final timeout-like error even when the measured error rate is negligible.",
	)
	fmt.Fprintln(
		b,
		"- Runs with error rate >= 1% are marked as rejection/error-path scenarios and should not be mixed "+
			"with success-heavy throughput comparisons.",
	)
	if hasTargetUnlimited(reports) {
		fmt.Fprintln(b, "- `target_rps = 0` means the client generated load without an explicit rate limit.")
	}
	fmt.Fprintln(b)
}

func filterReports(reports []benchmarkReport, keep func(*benchmarkReport) bool) []*benchmarkReport {
	filtered := make([]*benchmarkReport, 0, len(reports))
	for i := range reports {
		report := &reports[i]
		if keep(report) {
			filtered = append(filtered, report)
		}
	}

	return filtered
}

func findScenario(reports []benchmarkReport, commandPrefix, distribution string) (*benchmarkReport, bool) {
	for i := range reports {
		report := &reports[i]
		if strings.HasPrefix(report.Metadata.QueryTemplate, commandPrefix) &&
			report.Metadata.KeyDistribution == distribution {
			return report, true
		}
	}

	return nil, false
}

func scenarioName(report *benchmarkReport) string {
	name := strings.TrimSuffix(filepath.Base(report.Metadata.Profile), filepath.Ext(report.Metadata.Profile))
	if name != "" && name != "." {
		return name
	}

	return strings.TrimSuffix(filepath.Base(report.source), filepath.Ext(report.source))
}

func throughputDrop(baseline, current float64) float64 {
	if baseline <= 0 {
		return 0
	}

	return math.Max(0, 1-current/baseline)
}

func ratio(value, baseline float64) float64 {
	if baseline <= 0 {
		return 0
	}

	return value / baseline
}

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', 1, 64)
}

func formatRatio(v float64) string {
	return strconv.FormatFloat(v, 'f', 2, 64)
}

func formatPercent(v float64) string {
	return strconv.FormatFloat(v*percentMultiplier, 'f', 4, 64) + "%"
}

func formatMicros(micros int64) string {
	if micros >= 1000 {
		return strconv.FormatFloat(float64(micros)/1000, 'f', 2, 64) + " ms"
	}

	return strconv.FormatInt(micros, 10) + " us"
}

func formatUint(value uint64) string {
	return strconv.FormatUint(value, 10)
}

func formatTargetRPS(value float64) string {
	if value == 0 {
		return "unlimited"
	}

	return formatFloat(value)
}

func slashPath(path string) string {
	return filepath.ToSlash(path)
}

func escapeCell(value string) string {
	return strings.ReplaceAll(value, "|", "\\|")
}

func hasTargetUnlimited(reports []benchmarkReport) bool {
	for i := range reports {
		report := &reports[i]
		if report.Metadata.TargetRPS == 0 {
			return true
		}
	}

	return false
}
