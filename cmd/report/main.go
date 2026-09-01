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

type reportInput struct {
	benchmarkDir string
	runDir       string
	metadata     *resultsMetadata
	manifest     *resultsManifest
	serverInfo   *serverInfoReport
	stress       []stressReport
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

type resultsManifest struct {
	Metadata  resultsMetadata        `json:"metadata"`
	Artifacts resultsArtifacts       `json:"artifacts"`
	Commands  []resultsCommand       `json:"commands"`
	Results   []resultsCommandResult `json:"results,omitempty"`
}

type resultsArtifacts struct {
	ServerInfoPath string `json:"server_info_path,omitempty"`
}

type resultsMetadata struct {
	Mode           string            `json:"mode"`
	GitCommit      string            `json:"git_commit,omitempty"`
	GitDirty       bool              `json:"git_dirty"`
	Hostname       string            `json:"hostname,omitempty"`
	Machine        string            `json:"machine"`
	GOOS           string            `json:"goos"`
	GOARCH         string            `json:"goarch"`
	GoVersion      string            `json:"go_version"`
	NumCPU         int               `json:"num_cpu"`
	Environment    map[string]string `json:"environment,omitempty"`
	System         map[string]string `json:"system,omitempty"`
	ConfigSHA256   map[string]string `json:"config_sha256,omitempty"`
	GeneratedAt    time.Time         `json:"generated_at"`
	RepositoryRoot string            `json:"repository_root"`
}

type resultsCommand struct {
	Name       string   `json:"name"`
	Kind       string   `json:"kind"`
	Command    []string `json:"command"`
	OutputFile string   `json:"output_file,omitempty"`
	Duration   string   `json:"duration,omitempty"`
}

type resultsCommandResult struct {
	Name     string    `json:"name"`
	ExitCode int       `json:"exit_code"`
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
	LogPath  string    `json:"log_path"`
	Error    string    `json:"error,omitempty"`
}

type stressReport struct {
	Scenario       string       `json:"scenario"`
	Status         string       `json:"status"`
	StartedAt      time.Time    `json:"started_at"`
	FinishedAt     time.Time    `json:"finished_at"`
	DurationMillis int64        `json:"duration_millis"`
	Result         stressResult `json:"result"`
	Failure        string       `json:"failure,omitempty"`
	Environment    stressEnv    `json:"environment"`
	source         string
}

type stressResult struct {
	Scenario        string `json:"scenario"`
	Address         string `json:"address"`
	SlaveAddress    string `json:"slave_address"`
	Operations      uint64 `json:"operations"`
	Restarts        int    `json:"restarts"`
	Dumps           int    `json:"dumps"`
	TransientErrors uint64 `json:"transient_errors"`
}

type stressEnv struct {
	ConfigPath string `json:"config_path"`
	WALDir     string `json:"wal_dir"`
	DumpDir    string `json:"dump_dir"`
	ReportPath string `json:"report_path"`
	Address    string `json:"address"`
}

type serverInfoReport struct {
	Instance    *serverInstanceInfo    `json:"instance,omitempty"`
	Persistence *serverPersistenceInfo `json:"persistence,omitempty"`
	WAL         *serverWALInfo         `json:"wal,omitempty"`
	Repl        *serverReplInfo        `json:"repl,omitempty"`
	Engine      *serverEngineInfo      `json:"engine,omitempty"`
}

type serverInstanceInfo struct {
	Version    string `json:"version"`
	Commit     string `json:"commit"`
	BuildDate  string `json:"build_date"`
	GoVersion  string `json:"go_version"`
	Platform   string `json:"platform"`
	Hostname   string `json:"hostname"`
	NumCPU     int    `json:"num_cpu"`
	Role       string `json:"role"`
	ListenAddr string `json:"listen_addr"`
}

type serverPersistenceInfo struct {
	Mode       string  `json:"mode"`
	SyncCommit *string `json:"sync_commit"`
}

type serverWALInfo struct {
	Enabled       bool    `json:"enabled"`
	DataDirectory *string `json:"data_directory"`
}

type serverReplInfo struct {
	Role            string `json:"role"`
	ProtocolVersion int    `json:"protocol_version"`
}

type serverEngineInfo struct {
	Partitions      int  `json:"partitions"`
	KeyIndexEnabled bool `json:"key_index_enabled"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "report failed:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	cfg := parseFlags(args)
	input, err := discoverInput(cfg.inputDir)
	if err != nil {
		return err
	}
	reports, err := loadReports(input.benchmarkDir)
	if err != nil {
		return err
	}
	if len(reports) == 0 {
		return fmt.Errorf("no benchmark JSON reports found in %s", input.benchmarkDir)
	}

	sortReports(reports)
	report := renderMarkdown(cfg, input, reports)
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
	flags.StringVar(&cfg.inputDir, "input_dir", defaultInputDir, "alias for -input")
	flags.StringVar(&cfg.outputFile, "output", "", "markdown report output path; use - for stdout")
	flags.StringVar(&cfg.outputFile, "output_file", "", "alias for -output")
	flags.StringVar(&cfg.title, "title", "FQ Benchmark Report", "markdown report title")
	flags.StringVar(&cfg.serverMachine, "server_machine", "remote Linux server", "database server label")
	flags.StringVar(&cfg.clientMachine, "client_machine", "remote Linux benchmark client", "benchmark client label")
	flags.IntVar(&cfg.clientCPU, "client_cpu", defaultClientCPU, "benchmark client CPU count")
	flags.StringVar(&cfg.persistence, "persistence", "WAL + dump, sync_commit=off", "persistence mode description")
	flags.StringVar(&cfg.notes, "notes", "", "extra markdown sentence added to the methodology section")
	_ = flags.Parse(args)

	if cfg.outputFile == "" {
		cfg.outputFile = defaultOutputPath(time.Now())
	}

	return cfg
}

func defaultOutputPath(now time.Time) string {
	return filepath.Join("benchmarks", "reports", "report_"+now.Format("2006_01_02")+".md")
}

func discoverInput(inputDir string) (reportInput, error) {
	input := reportInput{benchmarkDir: inputDir}
	benchmarkDir := filepath.Join(inputDir, "benchmarks")
	if info, err := os.Stat(benchmarkDir); err == nil && info.IsDir() {
		input.runDir = inputDir
		input.benchmarkDir = benchmarkDir
		metadata, err := loadOptionalJSON[resultsMetadata](filepath.Join(inputDir, "metadata.json"))
		if err != nil {
			return reportInput{}, err
		}

		input.metadata = metadata

		manifest, err := loadOptionalJSON[resultsManifest](filepath.Join(inputDir, "manifest.json"))
		if err != nil {
			return reportInput{}, err
		}

		input.manifest = manifest

		serverInfo, err := loadServerInfo(inputDir, input.manifest)
		if err != nil {
			return reportInput{}, err
		}
		input.serverInfo = serverInfo

		stressReports, err := loadStressReports(filepath.Join(inputDir, "stress"))
		if err != nil {
			return reportInput{}, err
		}
		input.stress = stressReports
	}

	return input, nil
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

func loadStressReports(inputDir string) ([]stressReport, error) {
	entries, err := os.ReadDir(inputDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, fmt.Errorf("read stress directory: %w", err)
	}

	reports := make([]stressReport, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		path := filepath.Join(inputDir, entry.Name())
		report, err := loadJSON[stressReport](path)
		if err != nil {
			return nil, err
		}
		report.source = path
		reports = append(reports, report)
	}
	sort.Slice(reports, func(i, j int) bool {
		return reports[i].source < reports[j].source
	})

	return reports, nil
}

func loadServerInfo(runDir string, manifest *resultsManifest) (*serverInfoReport, error) {
	paths := []string{filepath.Join(runDir, "server-info.json")}
	if manifest != nil && manifest.Artifacts.ServerInfoPath != "" {
		paths = append([]string{manifest.Artifacts.ServerInfoPath}, paths...)
	}

	for _, path := range paths {
		info, err := loadOptionalJSON[serverInfoReport](path)
		if err != nil {
			return nil, err
		}
		if info != nil {
			return info, nil
		}
	}

	return nil, nil
}

func loadOptionalJSON[T any](path string) (*T, error) {
	value, err := loadJSON[T](path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}

		return nil, err
	}

	return &value, nil
}

func loadJSON[T any](path string) (T, error) {
	var value T
	data, err := os.ReadFile(path)
	if err != nil {
		return value, fmt.Errorf("read %s: %w", path, err)
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return value, fmt.Errorf("parse %s: %w", path, err)
	}

	return value, nil
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

func renderMarkdown(cfg config, input reportInput, reports []benchmarkReport) string {
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
	renderRunMetadata(&b, input)
	renderEnvironment(&b, cfg, input, reports)
	renderMethodology(&b, cfg, input, reports)
	renderManifest(&b, input)
	renderHeadlineTable(&b, reports)
	renderWorkloadTable(&b, reports)
	renderLatencyTable(&b, reports)
	renderComparisonTable(&b, reports)
	renderStressTable(&b, input)
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

func renderRunMetadata(b *strings.Builder, input reportInput) {
	if input.metadata == nil && input.manifest == nil && input.runDir == "" {
		return
	}

	meta := input.metadata
	if meta == nil && input.manifest != nil {
		meta = &input.manifest.Metadata
	}
	if meta == nil {
		return
	}

	fmt.Fprintln(b, "## Release Run Metadata")
	fmt.Fprintln(b)
	fmt.Fprintln(b, "| Field | Value |")
	fmt.Fprintln(b, "| --- | --- |")
	fmt.Fprintf(b, "| Run directory | `%s` |\n", slashPath(input.runDir))
	fmt.Fprintf(b, "| Mode | `%s` |\n", meta.Mode)
	fmt.Fprintf(b, "| Git commit | `%s` |\n", meta.GitCommit)
	fmt.Fprintf(b, "| Git dirty | `%t` |\n", meta.GitDirty)
	fmt.Fprintf(b, "| Machine | `%s` |\n", meta.Machine)
	if !meta.GeneratedAt.IsZero() {
		fmt.Fprintf(b, "| Generated at | `%s` |\n", meta.GeneratedAt.UTC().Format(time.RFC3339))
	}
	if meta.Hostname != "" {
		fmt.Fprintf(b, "| Hostname | `%s` |\n", meta.Hostname)
	}
	if len(meta.ConfigSHA256) > 0 {
		keys := sortedKeys(meta.ConfigSHA256)
		for _, key := range keys {
			fmt.Fprintf(b, "| Config SHA-256 `%s` | `%s` |\n", slashPath(key), meta.ConfigSHA256[key])
		}
	}
	if len(meta.Environment) > 0 {
		for _, key := range sortedKeys(meta.Environment) {
			fmt.Fprintf(b, "| Env `%s` | `%s` |\n", key, escapeCell(meta.Environment[key]))
		}
	}
	if input.serverInfo != nil && input.serverInfo.Instance != nil {
		info := input.serverInfo.Instance
		fmt.Fprintf(b, "| Database version | `%s` |\n", info.Version)
		fmt.Fprintf(b, "| Database commit | `%s` |\n", info.Commit)
		if info.BuildDate != "" {
			fmt.Fprintf(b, "| Database build date | `%s` |\n", info.BuildDate)
		}
	}
	if input.serverInfo != nil && input.serverInfo.Repl != nil {
		fmt.Fprintf(b, "| Database replication role | `%s` |\n", input.serverInfo.Repl.Role)
		fmt.Fprintf(b, "| Replication protocol version | `%d` |\n", input.serverInfo.Repl.ProtocolVersion)
	}
	fmt.Fprintln(b)
}

func renderEnvironment(b *strings.Builder, cfg config, input reportInput, reports []benchmarkReport) {
	first := reports[0]
	dbMachine := cfg.serverMachine
	dbCPU := first.Metadata.NumCPU
	dbPartitions := 0
	dbPlatform := first.Metadata.GOOS + "/" + first.Metadata.GOARCH
	dbGoVersion := first.Metadata.GoVersion
	dbPersistence := cfg.persistence
	dbRole := ""
	dbListen := ""
	clientMachine := cfg.clientMachine
	clientCPU := cfg.clientCPU
	clientPlatform := first.Metadata.GOOS + "/" + first.Metadata.GOARCH
	clientGoVersion := first.Metadata.GoVersion
	if input.metadata != nil {
		clientMachine = input.metadata.Machine
		clientCPU = input.metadata.NumCPU
		clientPlatform = input.metadata.GOOS + "/" + input.metadata.GOARCH
		clientGoVersion = input.metadata.GoVersion
		if dbMachine == "remote Linux server" && input.metadata.Machine != "" {
			dbMachine = input.metadata.Machine
		}
	}
	if input.serverInfo != nil {
		if input.serverInfo.Instance != nil {
			if input.serverInfo.Instance.Role != "" {
				dbRole = input.serverInfo.Instance.Role
			}
			if input.serverInfo.Instance.ListenAddr != "" {
				dbListen = input.serverInfo.Instance.ListenAddr
			}
			if input.serverInfo.Instance.Platform != "" {
				dbPlatform = input.serverInfo.Instance.Platform
			}
			if input.serverInfo.Instance.GoVersion != "" {
				dbGoVersion = input.serverInfo.Instance.GoVersion
			}
			if input.serverInfo.Instance.NumCPU > 0 {
				dbCPU = input.serverInfo.Instance.NumCPU
			}
			if input.serverInfo.Instance.Hostname != "" {
				dbMachine = input.serverInfo.Instance.Hostname
			}
		}
		if input.serverInfo.Engine != nil && input.serverInfo.Engine.Partitions > 0 {
			dbPartitions = input.serverInfo.Engine.Partitions
		}
		if input.serverInfo.Persistence != nil && input.serverInfo.Persistence.Mode != "" {
			dbPersistence = input.serverInfo.Persistence.Mode
			if input.serverInfo.Persistence.SyncCommit != nil {
				dbPersistence += ", sync_commit=" + *input.serverInfo.Persistence.SyncCommit
			}
		}
	}
	fmt.Fprintln(b, "## Environment")
	fmt.Fprintln(b)
	fmt.Fprintln(b, "| Component | Value |")
	fmt.Fprintln(b, "| --- | --- |")
	fmt.Fprintf(b, "| Database server | %s |\n", escapeCell(dbMachine))
	if dbRole != "" {
		fmt.Fprintf(b, "| Database role | %s |\n", escapeCell(dbRole))
	}
	if dbListen != "" {
		fmt.Fprintf(b, "| Database listen address | `%s` |\n", escapeCell(dbListen))
	}
	fmt.Fprintf(b, "| Database CPU | %d |\n", dbCPU)
	if dbPartitions > 0 {
		fmt.Fprintf(b, "| Database partitions | %d |\n", dbPartitions)
	}
	fmt.Fprintf(b, "| Database OS / Arch | %s |\n", escapeCell(dbPlatform))
	fmt.Fprintf(b, "| Database Go version | %s |\n", escapeCell(dbGoVersion))
	fmt.Fprintf(b, "| Benchmark client | %s |\n", escapeCell(clientMachine))
	fmt.Fprintf(b, "| Benchmark client CPU | %d |\n", clientCPU)
	fmt.Fprintf(b, "| Benchmark client OS / Arch | %s |\n", escapeCell(clientPlatform))
	fmt.Fprintf(b, "| Benchmark client Go version | %s |\n", escapeCell(clientGoVersion))
	fmt.Fprintf(b, "| Persistence | %s |\n", escapeCell(dbPersistence))
	fmt.Fprintln(b)
}

func renderMethodology(b *strings.Builder, cfg config, input reportInput, reports []benchmarkReport) {
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
	if input.runDir != "" {
		fmt.Fprintf(
			b,
			"This report was rendered from the reproducible results run directory `%s`; "+
				"the run contains metadata, manifest, logs, benchmark JSON, stress JSON, and config/profile snapshots.\n\n",
			slashPath(input.runDir),
		)
	}
}

func renderManifest(b *strings.Builder, input reportInput) {
	if input.manifest == nil || len(input.manifest.Commands) == 0 {
		return
	}

	fmt.Fprintln(b, "## Command Manifest")
	fmt.Fprintln(b)
	fmt.Fprintln(b, "| Name | Kind | Command | Output | Status |")
	fmt.Fprintln(b, "| --- | --- | --- | --- | --- |")
	results := make(map[string]resultsCommandResult, len(input.manifest.Results))
	for _, result := range input.manifest.Results {
		results[result.Name] = result
	}
	for _, command := range input.manifest.Commands {
		status := "planned"
		if result, ok := results[command.Name]; ok {
			status = "ok"
			if result.ExitCode != 0 {
				status = "failed: exit code " + strconv.Itoa(result.ExitCode)
			}
			if result.Error != "" {
				status = "failed: " + result.Error
			}
		}
		fmt.Fprintf(
			b,
			"| `%s` | %s | `%s` | `%s` | %s |\n",
			command.Name,
			command.Kind,
			escapeCell(strings.Join(command.Command, " ")),
			slashPath(command.OutputFile),
			escapeCell(status),
		)
	}
	fmt.Fprintln(b)
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

func renderStressTable(b *strings.Builder, input reportInput) {
	if len(input.stress) == 0 {
		return
	}

	fmt.Fprintln(b, "## Stress Results")
	fmt.Fprintln(b)
	fmt.Fprintln(b, "| Scenario | Status | Operations | Restarts | Dumps | Transient errors | Duration | Source |")
	fmt.Fprintln(b, "| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |")
	for _, report := range input.stress { //nolint:gocritic // ok for tool
		duration := time.Duration(report.DurationMillis) * time.Millisecond
		if report.DurationMillis == 0 && !report.StartedAt.IsZero() && !report.FinishedAt.IsZero() {
			duration = report.FinishedAt.Sub(report.StartedAt)
		}
		status := report.Status
		if report.Failure != "" {
			status += ": " + report.Failure
		}
		fmt.Fprintf(
			b,
			"| `%s` | %s | %d | %d | %d | %d | %s | `%s` |\n",
			report.Scenario,
			escapeCell(status),
			report.Result.Operations,
			report.Result.Restarts,
			report.Result.Dumps,
			report.Result.TransientErrors,
			duration.Round(time.Millisecond),
			slashPath(report.source),
		)
	}
	fmt.Fprintln(b)
}

//nolint:lll // ok
func renderNotes(b *strings.Builder, reports []benchmarkReport) {
	fmt.Fprintln(b, "## Interpretation Notes")
	fmt.Fprintln(b)
	fmt.Fprintln(b, "- `server-info.json` is preferred for database-server metadata; benchmark metadata is used as a fallback.")
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

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return keys
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
