package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	modeSmoke        = "smoke"
	modeRelease      = "release"
	flagKillInterval = "-kill_interval"
)

type config struct {
	mode              string
	outputRoot        string
	machine           string
	address           string
	run               bool
	includeBenchmarks bool
	includeStress     bool
	confirmReleaseRun bool
}

type runManifest struct {
	Metadata  metadata     `json:"metadata"`
	Artifacts artifacts    `json:"artifacts"`
	Commands  []runCommand `json:"commands"`
	Results   []runResult  `json:"results,omitempty"`
	CreatedAt time.Time    `json:"created_at"`
	Notes     []string     `json:"notes,omitempty"`
}

type metadata struct {
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

type artifacts struct {
	RunDir       string `json:"run_dir"`
	BenchDir     string `json:"bench_dir"`
	StressDir    string `json:"stress_dir"`
	SnapshotDir  string `json:"snapshot_dir"`
	MetadataPath string `json:"metadata_path"`
	ManifestPath string `json:"manifest_path"`
	SummaryPath  string `json:"summary_path"`
}

type runCommand struct {
	Name       string   `json:"name"`
	Kind       string   `json:"kind"`
	Command    []string `json:"command"`
	OutputFile string   `json:"output_file,omitempty"`
	Duration   string   `json:"duration,omitempty"`
}

type runResult struct {
	Name     string    `json:"name"`
	ExitCode int       `json:"exit_code"`
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
	LogPath  string    `json:"log_path"`
	Error    string    `json:"error,omitempty"`
}

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "results failed:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	cfg := parseFlags(args)
	if cfg.mode != modeSmoke && cfg.mode != modeRelease {
		return fmt.Errorf("unknown mode %q, want %q or %q", cfg.mode, modeSmoke, modeRelease)
	}
	if cfg.mode == modeRelease && cfg.run && !cfg.confirmReleaseRun {
		return errors.New("release run is heavy; pass -confirm_release_run to execute it")
	}

	repoRoot, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get working directory: %w", err)
	}

	meta := collectMetadata(repoRoot, cfg)
	paths, err := createArtifacts(cfg.outputRoot, meta)
	if err != nil {
		return err
	}
	manifest := runManifest{
		Metadata:  meta,
		Artifacts: paths,
		Commands:  buildCommands(cfg, paths),
		CreatedAt: time.Now(),
	}
	if !cfg.run {
		manifest.Notes = append(manifest.Notes, "dry run: commands were planned but not executed")
	}

	if err := writeJSON(paths.MetadataPath, meta); err != nil {
		return err
	}
	if err := copySnapshots(paths.SnapshotDir); err != nil {
		return err
	}
	if cfg.run {
		results, err := executeCommands(ctx, manifest.Commands, paths.RunDir)
		manifest.Results = results
		if writeErr := writeManifestAndSummary(manifest); writeErr != nil {
			return writeErr
		}
		if err != nil {
			return err
		}
	} else if err := writeManifestAndSummary(manifest); err != nil {
		return err
	}

	fmt.Printf("results run directory: %s\n", paths.RunDir)
	fmt.Printf("manifest: %s\n", paths.ManifestPath)
	fmt.Printf("summary: %s\n", paths.SummaryPath)

	return nil
}

func parseFlags(args []string) config {
	cfg := config{}
	flags := flag.NewFlagSet("results", flag.ExitOnError)
	flags.StringVar(&cfg.mode, "mode", modeSmoke, "results mode: smoke or release")
	flags.StringVar(&cfg.outputRoot, "output_root", "benchmarks/results/runs", "directory for timestamped result runs")
	flags.StringVar(&cfg.machine, "machine", "", "machine label; empty uses hostname")
	flags.StringVar(&cfg.address, "address", ":1945", "fq server address for benchmark commands")
	flags.BoolVar(&cfg.run, "run", false, "execute planned commands; false only writes metadata and manifest")
	flags.BoolVar(&cfg.includeBenchmarks, "benchmarks", true, "include benchmark commands in the plan")
	flags.BoolVar(&cfg.includeStress, "stress", true, "include stress commands in the plan")
	flags.BoolVar(&cfg.confirmReleaseRun, "confirm_release_run", false, "allow executing release mode")
	_ = flags.Parse(args)

	return cfg
}

func collectMetadata(repoRoot string, cfg config) metadata {
	hostname, _ := os.Hostname()
	machine := cfg.machine
	if machine == "" {
		machine = hostname
	}

	return metadata{
		Mode:           cfg.mode,
		GitCommit:      commandOutput("git", "rev-parse", "HEAD"),
		GitDirty:       commandOutput("git", "status", "--porcelain") != "",
		Hostname:       hostname,
		Machine:        sanitizeName(machine),
		GOOS:           runtime.GOOS,
		GOARCH:         runtime.GOARCH,
		GoVersion:      runtime.Version(),
		NumCPU:         runtime.NumCPU(),
		Environment:    selectedEnvironment(),
		System:         systemMetadata(),
		ConfigSHA256:   fileHashes("config.yml", "config-slave.yml"),
		GeneratedAt:    time.Now(),
		RepositoryRoot: repoRoot,
	}
}

func createArtifacts(outputRoot string, meta metadata) (artifacts, error) {
	shortCommit := "unknown"
	if len(meta.GitCommit) >= 12 {
		shortCommit = meta.GitCommit[:12]
	}
	runID := fmt.Sprintf(
		"%s-%s-%s-%s",
		meta.GeneratedAt.UTC().Format("20060102T150405Z"),
		meta.Machine,
		shortCommit,
		meta.Mode,
	)
	runDir := filepath.Join(outputRoot, runID)
	paths := artifacts{
		RunDir:       runDir,
		BenchDir:     filepath.Join(runDir, "benchmarks"),
		StressDir:    filepath.Join(runDir, "stress"),
		SnapshotDir:  filepath.Join(runDir, "snapshots"),
		MetadataPath: filepath.Join(runDir, "metadata.json"),
		ManifestPath: filepath.Join(runDir, "manifest.json"),
		SummaryPath:  filepath.Join(runDir, "summary.md"),
	}

	for _, dir := range []string{paths.BenchDir, paths.StressDir, paths.SnapshotDir} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return artifacts{}, fmt.Errorf("create %s: %w", dir, err)
		}
	}

	return paths, nil
}

func buildCommands(cfg config, paths artifacts) []runCommand {
	var commands []runCommand
	if cfg.includeBenchmarks {
		for _, profile := range benchmarkProfiles(cfg.mode) {
			name := strings.TrimSuffix(filepath.Base(profile), filepath.Ext(profile))
			outputFile := filepath.Join(paths.BenchDir, name+".json")
			args := []string{
				"go", "run", "./cmd/bench",
				"-profile", profile,
				"-address", cfg.address,
				"-output_file", outputFile,
			}
			if cfg.mode == modeSmoke {
				args = append(args, "-warmup", "1s", "-duration", "3s", "-connections", "8", "-key_range", "1000")
			}
			commands = append(commands, runCommand{
				Name:       "bench-" + name,
				Kind:       "benchmark",
				Command:    args,
				OutputFile: outputFile,
			})
		}
	}

	if cfg.includeStress {
		commands = append(commands, stressCommands(cfg.mode, paths.StressDir)...)
	}

	return commands
}

func benchmarkProfiles(mode string) []string {
	if mode == modeSmoke {
		return []string{"benchmarks/profiles/smoke.yml"}
	}

	return []string{
		"benchmarks/profiles/release-hot-counter.yml",
		"benchmarks/profiles/release-uniform-counter.yml",
		"benchmarks/profiles/release-fw.yml",
		"benchmarks/profiles/release-sw.yml",
		"benchmarks/profiles/release-tb.yml",
	}
}

func stressCommands(mode, stressDir string) []runCommand {
	duration := "30s"
	workers := "4"
	keys := "100"
	killInterval := "2s"
	if mode == modeSmoke {
		duration = "3s"
		workers = "2"
		keys = "10"
		killInterval = "500ms"
	}

	defs := []struct {
		name  string
		extra []string
	}{
		{name: "crash-loop", extra: []string{flagKillInterval, killInterval}},
		{name: "dump-recovery", extra: []string{flagKillInterval, killInterval, "-dump_interval", "250ms"}},
		{name: "replication-stress", extra: []string{flagKillInterval, killInterval, "-sync_interval", "100ms"}},
	}

	commands := make([]runCommand, 0, len(defs))
	for _, def := range defs {
		outputFile := filepath.Join(stressDir, def.name+".json")
		args := []string{
			"go", "run", "./cmd/stress",
			"-scenario", def.name,
			"-duration", duration,
			"-workers", workers,
			"-keys", keys,
			"-seed", "42",
			"-report_file", outputFile,
		}
		args = append(args, def.extra...)
		commands = append(commands, runCommand{
			Name:       "stress-" + def.name,
			Kind:       "stress",
			Command:    args,
			OutputFile: outputFile,
			Duration:   duration,
		})
	}

	return commands
}

func executeCommands(ctx context.Context, commands []runCommand, runDir string) ([]runResult, error) {
	results := make([]runResult, 0, len(commands))
	var runErr error
	for _, command := range commands {
		result := runResult{
			Name:    command.Name,
			Started: time.Now(),
			LogPath: filepath.Join(runDir, command.Name+".log"),
		}
		err := executeCommand(ctx, command.Command, result.LogPath)
		result.Finished = time.Now()
		if err != nil {
			result.Error = err.Error()
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				result.ExitCode = exitErr.ExitCode()
			} else {
				result.ExitCode = -1
			}
			runErr = errors.Join(runErr, fmt.Errorf("%s: %w", command.Name, err))
		}
		results = append(results, result)
	}

	return results, runErr
}

func executeCommand(ctx context.Context, args []string, logPath string) error {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open command log: %w", err)
	}
	defer func() { _ = logFile.Close() }()

	cmd := exec.CommandContext(ctx, args[0], args[1:]...) //nolint:gosec // planned local release commands.
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	return cmd.Run()
}

func writeManifestAndSummary(manifest runManifest) error {
	if err := writeJSON(manifest.Artifacts.ManifestPath, manifest); err != nil {
		return err
	}

	return os.WriteFile(manifest.Artifacts.SummaryPath, []byte(formatSummary(manifest)), 0o644)
}

func formatSummary(manifest runManifest) string {
	var b strings.Builder
	b.WriteString("# fq Release Results\n\n")
	fmt.Fprintf(&b, "- mode: `%s`\n", manifest.Metadata.Mode)
	fmt.Fprintf(&b, "- git_commit: `%s`\n", manifest.Metadata.GitCommit)
	fmt.Fprintf(&b, "- git_dirty: `%t`\n", manifest.Metadata.GitDirty)
	fmt.Fprintf(&b, "- machine: `%s`\n", manifest.Metadata.Machine)
	fmt.Fprintf(&b, "- runtime: `%s/%s %s`, cpu=%d\n",
		manifest.Metadata.GOOS,
		manifest.Metadata.GOARCH,
		manifest.Metadata.GoVersion,
		manifest.Metadata.NumCPU,
	)
	fmt.Fprintf(&b, "- run_dir: `%s`\n\n", manifest.Artifacts.RunDir)

	b.WriteString("## Commands\n\n")
	for _, command := range manifest.Commands {
		fmt.Fprintf(&b, "- `%s`: `%s`\n", command.Name, strings.Join(command.Command, " "))
	}
	if len(manifest.Results) > 0 {
		b.WriteString("\n## Results\n\n")
		for _, result := range manifest.Results {
			status := "ok"
			if result.Error != "" {
				status = result.Error
			}
			fmt.Fprintf(&b, "- `%s`: %s, log `%s`\n", result.Name, status, result.LogPath)
		}
	}
	if len(manifest.Notes) > 0 {
		b.WriteString("\n## Notes\n\n")
		for _, note := range manifest.Notes {
			fmt.Fprintf(&b, "- %s\n", note)
		}
	}

	return b.String()
}

func writeJSON(path string, value interface{}) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", path, err)
	}
	data = append(data, '\n')

	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create %s dir: %w", path, err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}

	return nil
}

func copySnapshots(snapshotDir string) error {
	for _, path := range []string{
		"config.yml",
		"config-slave.yml",
		"Makefile",
		"benchmarks/profiles/smoke.yml",
		"benchmarks/profiles/release-hot-counter.yml",
		"benchmarks/profiles/release-uniform-counter.yml",
		"benchmarks/profiles/release-fw.yml",
		"benchmarks/profiles/release-sw.yml",
		"benchmarks/profiles/release-tb.yml",
	} {
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("read snapshot %s: %w", path, err)
		}
		dest := filepath.Join(snapshotDir, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(dest), 0o750); err != nil {
			return fmt.Errorf("create snapshot dir: %w", err)
		}
		if err := os.WriteFile(dest, data, 0o644); err != nil {
			return fmt.Errorf("write snapshot %s: %w", dest, err)
		}
	}

	return nil
}

func selectedEnvironment() map[string]string {
	out := make(map[string]string)
	for _, key := range []string{"GOMAXPROCS", "GOGC", "GOMEMLIMIT", "GOFLAGS"} {
		if value := os.Getenv(key); value != "" {
			out[key] = value
		}
	}

	return out
}

func systemMetadata() map[string]string {
	out := make(map[string]string)
	for _, item := range []struct {
		key  string
		args []string
	}{
		{key: "uname", args: []string{"uname", "-a"}},
		{key: "cpu_brand", args: []string{"sysctl", "-n", "machdep.cpu.brand_string"}},
		{key: "memory_bytes", args: []string{"sysctl", "-n", "hw.memsize"}},
	} {
		if value := commandOutput(item.args[0], item.args[1:]...); value != "" {
			out[item.key] = value
		}
	}

	return out
}

func fileHashes(paths ...string) map[string]string {
	hashes := make(map[string]string)
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		sum := sha256.Sum256(data)
		hashes[path] = hex.EncodeToString(sum[:])
	}

	return hashes
}

func commandOutput(name string, args ...string) string {
	cmd := exec.Command(name, args...) //nolint:gosec // fixed metadata commands.
	data, err := cmd.Output()
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(data))
}

func sanitizeName(text string) string {
	text = strings.ToLower(strings.TrimSpace(text))
	var b strings.Builder
	for _, r := range text {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-_")
	if out == "" {
		return "machine"
	}

	return out
}
