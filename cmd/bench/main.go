package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/guptarohit/asciigraph"

	"github.com/fq-db/fq/internal/network"
	"github.com/fq-db/fq/internal/tools"
)

const defaultQueryTemplate = "INCR {key} {batch}"

const (
	sequential = "sequential"
	uniform    = "uniform"
	zipfian    = "zipfian"
)

type benchConfig struct {
	address         string
	connections     int
	warmup          time.Duration
	duration        time.Duration
	rps             float64
	requestTimeout  time.Duration
	idleTimeout     time.Duration
	maxMessageSize  int
	queryTemplate   string
	keyPrefix       string
	keyDistribution string
	keyStart        uint64
	keyRange        uint64
	batchSize       uint64
	outputFormat    string
	outputFile      string
	seed            int64
}

type result struct {
	at      time.Time
	latency time.Duration
	err     error
	badResp bool
	errText string
}

type snapshot struct {
	elapsed       time.Duration
	measuring     bool
	windowElapsed time.Duration
	windowCount   int
	windowErrors  int
	totalCount    uint64
	totalErrors   uint64
	latencies     []time.Duration
	lastError     string
}

type runReport struct {
	Metadata reportMetadata `json:"metadata"`
	Summary  reportSummary  `json:"summary"`
}

type reportMetadata struct {
	StartedAt       time.Time `json:"started_at"`
	FinishedAt      time.Time `json:"finished_at"`
	GoVersion       string    `json:"go_version"`
	GOOS            string    `json:"goos"`
	GOARCH          string    `json:"goarch"`
	NumCPU          int       `json:"num_cpu"`
	ConfigHash      string    `json:"config_hash"`
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
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if cfg.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.warmup+cfg.duration)
		defer cancel()
	}

	results := make(chan result, max(65536, cfg.connections*1024))
	var wg sync.WaitGroup
	for id := 0; id < cfg.connections; id++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runWorker(ctx, cfg, workerID, results)
		}(id)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	runReporter(ctx, cfg, results, done)
}

func parseFlags() (benchConfig, error) {
	address := flag.String("address", ":1945", "fq server address")
	connections := flag.Int("connections", 100, "number of concurrent TCP connections")
	warmup := flag.Duration("warmup", 0, "warmup duration excluded from final reported metrics")
	duration := flag.Duration("duration", 30*time.Second, "test duration; 0 means until Ctrl+C")
	rps := flag.Float64("rps", 0, "target total requests per second; 0 means unlimited")
	requestTimeout := flag.Duration("request_timeout", 5*time.Second, "timeout for one request")
	idleTimeout := flag.Duration("idle_timeout", time.Minute, "TCP connection idle timeout")
	maxMessageSizeStr := flag.String("max_message_size", "4KB", "max message size")
	queryTemplate := flag.String("query", defaultQueryTemplate, "query template; supports {key}, {batch}, {worker}, {n}")
	keyPrefix := flag.String("key_prefix", "bench", "key prefix for generated queries")
	keyDistribution := flag.String("key_distribution", sequential, "key distribution: sequential, uniform, or zipfian")
	keyStart := flag.Uint64("key_start", 0, "first generated key id")
	keyRange := flag.Uint64("key_range", 1000, "number of distinct generated keys")
	keys := flag.Uint64("keys", 0, "deprecated alias for key_range")
	batchSize := flag.Uint64("batch", 600, "batch value for the default query template")
	outputFormat := flag.String("output", "text", "final report format: text, json, or csv")
	outputFile := flag.String("output_file", "", "write final report to file instead of stdout")
	seed := flag.Int64("seed", 1, "benchmark seed recorded in report metadata")
	flag.Parse()

	if *connections <= 0 {
		return benchConfig{}, fmt.Errorf("connections must be positive")
	}
	if *warmup < 0 {
		return benchConfig{}, fmt.Errorf("warmup must be non-negative")
	}
	if *duration < 0 {
		return benchConfig{}, fmt.Errorf("duration must be non-negative")
	}
	if *rps < 0 {
		return benchConfig{}, fmt.Errorf("rps must be non-negative")
	}
	*outputFormat = strings.ToLower(*outputFormat)
	if *outputFormat != "text" && *outputFormat != "json" && *outputFormat != "csv" {
		return benchConfig{}, fmt.Errorf("output must be text, json, or csv")
	}
	*keyDistribution = strings.ToLower(*keyDistribution)
	if *keyDistribution != sequential && *keyDistribution != uniform && *keyDistribution != zipfian {
		return benchConfig{}, fmt.Errorf("key_distribution must be sequential, uniform, or zipfian")
	}
	if *keys != 0 {
		*keyRange = *keys
	}
	if *keyRange == 0 {
		return benchConfig{}, fmt.Errorf("key_range must be positive")
	}
	if *batchSize == 0 {
		return benchConfig{}, fmt.Errorf("batch must be positive")
	}

	maxMessageSize, err := tools.ParseSize(*maxMessageSizeStr)
	if err != nil {
		return benchConfig{}, fmt.Errorf("parse max_message_size: %w", err)
	}

	return benchConfig{
		address:         *address,
		connections:     *connections,
		warmup:          *warmup,
		duration:        *duration,
		rps:             *rps,
		requestTimeout:  *requestTimeout,
		idleTimeout:     *idleTimeout,
		maxMessageSize:  maxMessageSize,
		queryTemplate:   *queryTemplate,
		keyPrefix:       *keyPrefix,
		keyDistribution: *keyDistribution,
		keyStart:        *keyStart,
		keyRange:        *keyRange,
		batchSize:       *batchSize,
		outputFormat:    *outputFormat,
		outputFile:      *outputFile,
		seed:            *seed,
	}, nil
}

func runWorker(ctx context.Context, cfg benchConfig, workerID int, results chan<- result) {
	client, err := network.NewTCPClient(cfg.address, cfg.maxMessageSize, cfg.idleTimeout)
	if err != nil {
		sendResult(ctx, results, result{at: time.Now(), err: err, errText: err.Error()})
		return
	}
	defer func() { _ = client.Close() }()

	var interval time.Duration
	var next time.Time
	rng := rand.New(rand.NewSource(cfg.seed + int64(workerID)*7919)) //nolint:gosec // ok
	var zipf *rand.Zipf
	if cfg.keyDistribution == zipfian && cfg.keyRange > 1 {
		zipf = rand.NewZipf(rng, 1.1, 1, cfg.keyRange-1)
	}
	if cfg.rps > 0 {
		interval = time.Duration(float64(time.Second) * float64(cfg.connections) / cfg.rps)
		if interval < time.Nanosecond {
			interval = time.Nanosecond
		}
		stagger := time.Duration(float64(time.Second) * float64(workerID) / cfg.rps)
		next = time.Now().Add(stagger)
	}

	for n := uint64(0); ; n++ {
		if err := waitForPace(ctx, interval, &next); err != nil {
			return
		}

		query := makeQuery(cfg, workerID, n, rng, zipf)
		requestCtx, cancel := context.WithTimeout(ctx, cfg.requestTimeout)
		start := time.Now()
		response, err := client.Send(requestCtx, []byte(query))
		latency := time.Since(start)
		cancel()

		badResp := err == nil && !bytes.HasPrefix(response, []byte("ok|"))
		errText := ""
		if err != nil {
			errText = err.Error()
		} else if badResp {
			errText = string(response)
		}

		sendResult(ctx, results, result{
			at:      time.Now(),
			latency: latency,
			err:     err,
			badResp: badResp,
			errText: errText,
		})

		if err != nil {
			return
		}
	}
}

func waitForPace(ctx context.Context, interval time.Duration, next *time.Time) error {
	if interval <= 0 {
		return ctx.Err()
	}

	now := time.Now()
	if now.Before(*next) {
		timer := time.NewTimer(next.Sub(now))
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}

	*next = next.Add(interval)
	if next.Before(time.Now().Add(-interval)) {
		*next = time.Now().Add(interval)
	}

	return ctx.Err()
}

func makeQuery(cfg benchConfig, workerID int, n uint64, rng *rand.Rand, zipf *rand.Zipf) string {
	keyID := cfg.keyStart + nextKeyOffset(cfg, workerID, n, rng, zipf)
	replacer := strings.NewReplacer(
		"{key}", cfg.keyPrefix+"_"+strconv.FormatUint(keyID, 10),
		"{batch}", strconv.FormatUint(cfg.batchSize, 10),
		"{worker}", strconv.Itoa(workerID),
		"{n}", strconv.FormatUint(n, 10),
	)

	return replacer.Replace(cfg.queryTemplate)
}

func nextKeyOffset(cfg benchConfig, workerID int, n uint64, rng *rand.Rand, zipf *rand.Zipf) uint64 {
	switch cfg.keyDistribution {
	case uniform:
		return uint64(rng.Int63n(int64(cfg.keyRange)))
	case zipfian:
		if zipf == nil {
			return 0
		}

		return zipf.Uint64()
	default:
		return (uint64(workerID) + n*uint64(cfg.connections)) % cfg.keyRange
	}
}

func sendResult(ctx context.Context, results chan<- result, res result) {
	select {
	case results <- res:
	case <-ctx.Done():
	}
}

func runReporter(ctx context.Context, cfg benchConfig, results <-chan result, done <-chan struct{}) {
	start := time.Now()
	warmupEndsAt := start.Add(cfg.warmup)
	lastTick := start
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	activeResults := results
	var measuredCount uint64
	var measuredErrors uint64
	var windowCount int
	var windowErrors int
	var lastError string
	windowLatencies := make([]time.Duration, 0, 65536)
	measuredLatencies := make([]time.Duration, 0, 65536)
	rpsHistory := make([]float64, 0, 60)
	p99History := make([]float64, 0, 60)

	for {
		select {
		case res, ok := <-activeResults:
			if !ok {
				activeResults = nil
				continue
			}
			measuring := !res.at.Before(warmupEndsAt)
			recordResult(
				res,
				measuring,
				&measuredCount,
				&measuredErrors,
				&windowCount,
				&windowErrors,
				&windowLatencies,
				&measuredLatencies,
				&lastError,
			)
		case now := <-ticker.C:
			snap := snapshot{
				elapsed:       now.Sub(start),
				measuring:     !now.Before(warmupEndsAt),
				windowElapsed: now.Sub(lastTick),
				windowCount:   windowCount,
				windowErrors:  windowErrors,
				totalCount:    measuredCount,
				totalErrors:   measuredErrors,
				latencies:     windowLatencies,
				lastError:     lastError,
			}
			rps, p99 := renderSnapshot(cfg, snap, rpsHistory, p99History)
			rpsHistory = appendHistory(rpsHistory, rps, 60)
			p99History = appendHistory(p99History, p99, 60)
			lastTick = now
			windowCount = 0
			windowErrors = 0
			windowLatencies = make([]time.Duration, 0, 65536)
		case <-done:
			drainResults(
				results,
				warmupEndsAt,
				&measuredCount,
				&measuredErrors,
				&windowCount,
				&windowErrors,
				&windowLatencies,
				&measuredLatencies,
				&lastError,
			)
			now := time.Now()
			snap := snapshot{
				elapsed:       now.Sub(start),
				measuring:     !now.Before(warmupEndsAt),
				windowElapsed: now.Sub(lastTick),
				windowCount:   windowCount,
				windowErrors:  windowErrors,
				totalCount:    measuredCount,
				totalErrors:   measuredErrors,
				latencies:     windowLatencies,
				lastError:     lastError,
			}
			renderSnapshot(cfg, snap, rpsHistory, p99History)
			report := makeReport(cfg, start, now, measuredCount, measuredErrors, measuredLatencies, lastError)
			if err := writeReport(cfg, report); err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
			if cfg.outputFile != "" {
				fmt.Printf("\nReport written to %s\n", cfg.outputFile)
			}
			fmt.Println("\nDone.")
			return
		case <-ctx.Done():
			<-done
		}
	}
}

func recordResult(
	res result,
	measuring bool,
	measuredCount *uint64,
	measuredErrors *uint64,
	windowCount *int,
	windowErrors *int,
	windowLatencies *[]time.Duration,
	measuredLatencies *[]time.Duration,
	lastError *string,
) {
	if res.err != nil || res.badResp {
		*lastError = res.errText
	}
	if !measuring {
		return
	}

	*measuredCount++
	*windowCount++
	if res.err != nil || res.badResp {
		*measuredErrors++
		*windowErrors++
	}
	if res.latency > 0 {
		*windowLatencies = append(*windowLatencies, res.latency)
		*measuredLatencies = append(*measuredLatencies, res.latency)
	}
}

func drainResults(
	results <-chan result,
	warmupEndsAt time.Time,
	measuredCount *uint64,
	measuredErrors *uint64,
	windowCount *int,
	windowErrors *int,
	windowLatencies *[]time.Duration,
	measuredLatencies *[]time.Duration,
	lastError *string,
) {
	for {
		select {
		case res, ok := <-results:
			if !ok {
				return
			}
			measuring := !res.at.Before(warmupEndsAt)
			recordResult(
				res,
				measuring,
				measuredCount,
				measuredErrors,
				windowCount,
				windowErrors,
				windowLatencies,
				measuredLatencies,
				lastError,
			)
		default:
			return
		}
	}
}

//nolint:lll // ok
func renderSnapshot(cfg benchConfig, snap snapshot, rpsHistory, p99History []float64) (rps, p99dur float64) {
	latencies := append([]time.Duration(nil), snap.latencies...)
	slices.Sort(latencies)

	rps = float64(snap.windowCount) / math.Max(snap.windowElapsed.Seconds(), 0.001)
	errRate := float64(snap.windowErrors) / math.Max(snap.windowElapsed.Seconds(), 0.001)
	p50 := percentile(latencies, 50)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)
	p999 := percentileMillis(latencies, 99.9)
	maxLatency := time.Duration(0)
	if len(latencies) > 0 {
		maxLatency = latencies[len(latencies)-1]
	}

	fmt.Print("\033[H\033[2J")
	fmt.Println("fq bench")
	fmt.Println(strings.Repeat("=", 78))
	fmt.Printf("%-10s %s\n", "phase", formatRunState(cfg, snap))
	fmt.Printf("%-10s %s\n", "progress", formatProgress(cfg, snap.elapsed))
	fmt.Printf("%-10s %s\n", "target", fmt.Sprintf("%s rps, %d connections", formatTargetRPS(cfg.rps), cfg.connections))
	fmt.Printf("%-10s %s\n", "workload", fmt.Sprintf("%q", cfg.queryTemplate))
	fmt.Printf("%-10s %s\n", "keys", fmt.Sprintf("%s [%d,%d), seed=%d", cfg.keyDistribution, cfg.keyStart, cfg.keyStart+cfg.keyRange, cfg.seed))
	fmt.Println(strings.Repeat("-", 78))
	fmt.Println("current window")
	fmt.Printf("  %-14s %12.1f   %-14s %12.1f\n",
		"rps",
		rps,
		"errors/s",
		errRate,
	)
	fmt.Printf("  %-14s %12d   %-14s %12d\n",
		"requests",
		snap.windowCount,
		"errors",
		snap.windowErrors,
	)
	fmt.Printf("  %-14s %12s   %-14s %12s   %-14s %12s\n",
		"p50",
		formatDuration(p50),
		"p95",
		formatDuration(p95),
		"p99",
		formatDuration(p99),
	)
	fmt.Printf("  %-14s %12s   %-14s %12s\n",
		"p99.9",
		formatDuration(p999),
		"max",
		formatDuration(maxLatency),
	)
	fmt.Println(strings.Repeat("-", 78))
	fmt.Println("measured total")
	fmt.Printf("  %-14s %12d   %-14s %12d   %-14s %12.4f\n",
		"requests",
		snap.totalCount,
		"errors",
		snap.totalErrors,
		"error_rate",
		errorRatio(snap.totalCount, snap.totalErrors),
	)
	if snap.lastError != "" {
		fmt.Printf("  %-14s %s\n", "last_error", truncate(snap.lastError, 120))
	}
	fmt.Println(strings.Repeat("-", 78))
	fmt.Println(renderGraph("RPS", appendHistory(rpsHistory, rps, 60)))
	fmt.Println(renderGraph("p99 latency, ms", appendHistory(p99History, durationMillis(p99), 60)))
	fmt.Println(strings.Repeat("=", 78))
	fmt.Println("Stop: Ctrl+C")

	return rps, durationMillis(p99)
}

func makeReport(
	cfg benchConfig,
	startedAt time.Time,
	finishedAt time.Time,
	requests uint64,
	errors uint64,
	latencies []time.Duration,
	lastError string,
) runReport {
	sortedLatencies := append([]time.Duration(nil), latencies...)
	slices.Sort(sortedLatencies)

	measuredDuration := cfg.duration.Seconds()
	if cfg.duration == 0 {
		measuredDuration = finishedAt.Sub(startedAt.Add(cfg.warmup)).Seconds()
	}
	if measuredDuration < 0 {
		measuredDuration = 0
	}

	maxLatency := time.Duration(0)
	if len(sortedLatencies) > 0 {
		maxLatency = sortedLatencies[len(sortedLatencies)-1]
	}

	errorRate := 0.0
	if requests > 0 {
		errorRate = float64(errors) / float64(requests)
	}

	throughput := 0.0
	if measuredDuration > 0 {
		throughput = float64(requests) / measuredDuration
	}

	return runReport{
		Metadata: reportMetadata{
			StartedAt:       startedAt,
			FinishedAt:      finishedAt,
			GoVersion:       runtime.Version(),
			GOOS:            runtime.GOOS,
			GOARCH:          runtime.GOARCH,
			NumCPU:          runtime.NumCPU(),
			ConfigHash:      configHash(cfg),
			Address:         cfg.address,
			Connections:     cfg.connections,
			Warmup:          cfg.warmup.String(),
			Duration:        cfg.duration.String(),
			TargetRPS:       cfg.rps,
			RequestTimeout:  cfg.requestTimeout.String(),
			IdleTimeout:     cfg.idleTimeout.String(),
			MaxMessageSize:  cfg.maxMessageSize,
			QueryTemplate:   cfg.queryTemplate,
			KeyPrefix:       cfg.keyPrefix,
			KeyDistribution: cfg.keyDistribution,
			KeyStart:        cfg.keyStart,
			KeyRange:        cfg.keyRange,
			BatchSize:       cfg.batchSize,
			Seed:            cfg.seed,
		},
		Summary: reportSummary{
			MeasuredDurationSeconds: measuredDuration,
			Requests:                requests,
			Errors:                  errors,
			ErrorRate:               errorRate,
			ThroughputRPS:           throughput,
			Latency: latencySummary{
				P50Micros:  percentile(sortedLatencies, 50).Microseconds(),
				P95Micros:  percentile(sortedLatencies, 95).Microseconds(),
				P99Micros:  percentile(sortedLatencies, 99).Microseconds(),
				P999Micros: percentileMillis(sortedLatencies, 99.9).Microseconds(),
				MaxMicros:  maxLatency.Microseconds(),
			},
			LastError: lastError,
		},
	}
}

func writeReport(cfg benchConfig, report runReport) error {
	var data []byte
	var err error
	switch cfg.outputFormat {
	case "json":
		data, err = json.MarshalIndent(report, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal json report: %w", err)
		}
		data = append(data, '\n')
	case "csv":
		var b strings.Builder
		writer := csv.NewWriter(&b)
		if err := writer.Write([]string{
			"started_at",
			"finished_at",
			"config_hash",
			"requests",
			"errors",
			"error_rate",
			"throughput_rps",
			"p50_us",
			"p95_us",
			"p99_us",
			"p999_us",
			"max_us",
		}); err != nil {
			return err
		}
		if err := writer.Write([]string{
			report.Metadata.StartedAt.Format(time.RFC3339Nano),
			report.Metadata.FinishedAt.Format(time.RFC3339Nano),
			report.Metadata.ConfigHash,
			strconv.FormatUint(report.Summary.Requests, 10),
			strconv.FormatUint(report.Summary.Errors, 10),
			strconv.FormatFloat(report.Summary.ErrorRate, 'f', 6, 64),
			strconv.FormatFloat(report.Summary.ThroughputRPS, 'f', 2, 64),
			strconv.FormatInt(report.Summary.Latency.P50Micros, 10),
			strconv.FormatInt(report.Summary.Latency.P95Micros, 10),
			strconv.FormatInt(report.Summary.Latency.P99Micros, 10),
			strconv.FormatInt(report.Summary.Latency.P999Micros, 10),
			strconv.FormatInt(report.Summary.Latency.MaxMicros, 10),
		}); err != nil {
			return err
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			return err
		}
		data = []byte(b.String())
	default:
		data = []byte(formatTextReport(report))
	}

	if cfg.outputFile != "" {
		return os.WriteFile(cfg.outputFile, data, 0o644)
	}

	fmt.Print(string(data))

	return nil
}

func formatTextReport(report runReport) string {
	var b strings.Builder
	b.WriteString("\nfq bench final report\n")
	b.WriteString(strings.Repeat("=", 78))
	b.WriteByte('\n')
	fmt.Fprintf(&b, "%-14s %s\n", "config_hash", report.Metadata.ConfigHash)
	fmt.Fprintf(&b, "%-14s %s/%s, %s, cpu=%d\n",
		"runtime",
		report.Metadata.GOOS,
		report.Metadata.GOARCH,
		report.Metadata.GoVersion,
		report.Metadata.NumCPU,
	)
	fmt.Fprintf(&b, "%-14s %s, %d connections, target=%s rps\n",
		"target",
		report.Metadata.Address,
		report.Metadata.Connections,
		formatFloatTarget(report.Metadata.TargetRPS),
	)
	fmt.Fprintf(&b, "%-14s warmup=%s, measure=%s, keys=%s [%d,%d), seed=%d\n",
		"workload",
		report.Metadata.Warmup,
		report.Metadata.Duration,
		report.Metadata.KeyDistribution,
		report.Metadata.KeyStart,
		report.Metadata.KeyStart+report.Metadata.KeyRange,
		report.Metadata.Seed,
	)
	b.WriteString(strings.Repeat("-", 78))
	b.WriteByte('\n')
	fmt.Fprintf(&b, "%-14s %d\n", "requests", report.Summary.Requests)
	fmt.Fprintf(&b, "%-14s %d (%s)\n", "errors", report.Summary.Errors, formatRatio(report.Summary.ErrorRate))
	fmt.Fprintf(&b, "%-14s %.1f rps\n", "throughput", report.Summary.ThroughputRPS)
	fmt.Fprintf(&b, "%-14s p50=%s  p95=%s  p99=%s  p99.9=%s  max=%s\n",
		"latency",
		formatDuration(time.Duration(report.Summary.Latency.P50Micros)*time.Microsecond),
		formatDuration(time.Duration(report.Summary.Latency.P95Micros)*time.Microsecond),
		formatDuration(time.Duration(report.Summary.Latency.P99Micros)*time.Microsecond),
		formatDuration(time.Duration(report.Summary.Latency.P999Micros)*time.Microsecond),
		formatDuration(time.Duration(report.Summary.Latency.MaxMicros)*time.Microsecond),
	)
	if report.Summary.LastError != "" {
		fmt.Fprintf(&b, "%-14s %s\n", "last_error", truncate(report.Summary.LastError, 140))
	}
	b.WriteString(strings.Repeat("=", 78))
	b.WriteByte('\n')

	return b.String()
}

func truncate(s string, limit int) string {
	if len(s) <= limit {
		return s
	}

	return s[:limit-3] + "..."
}

func percentile(sorted []time.Duration, pct int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if pct <= 0 {
		return sorted[0]
	}
	if pct >= 100 {
		return sorted[len(sorted)-1]
	}

	idx := int(math.Ceil(float64(pct)/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}

	return sorted[idx]
}

func percentileMillis(sorted []time.Duration, pct float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	if pct <= 0 {
		return sorted[0]
	}
	if pct >= 100 {
		return sorted[len(sorted)-1]
	}

	idx := int(math.Ceil(pct/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}

	return sorted[idx]
}

//nolint:unparam // ok
func appendHistory(history []float64, value float64, limit int) []float64 {
	history = append(history, value)
	if len(history) > limit {
		copy(history, history[len(history)-limit:])
		history = history[:limit]
	}

	return history
}

func renderGraph(caption string, values []float64) string {
	if len(values) == 0 {
		return caption + ": no data"
	}

	return asciigraph.Plot(
		values,
		asciigraph.Width(60),
		asciigraph.Height(7),
		asciigraph.Caption(caption),
		asciigraph.Precision(1),
	)
}

func durationMillis(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

func formatDuration(d time.Duration) string {
	if d == 0 {
		return "-"
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.0fus", float64(d.Microseconds()))
	}

	return d.Round(time.Microsecond).String()
}

func formatTargetRPS(rps float64) string {
	if rps == 0 {
		return "unlimited"
	}

	return fmt.Sprintf("%.1f", rps)
}

func formatRunState(cfg benchConfig, snap snapshot) string {
	state := "measure"
	if cfg.warmup > 0 && !snap.measuring {
		state = "warmup"
	}
	if snap.lastError != "" && snap.windowCount == 0 && snap.totalCount == 0 {
		state = "waiting/error"
	}

	return fmt.Sprintf("%s, elapsed=%s", state, snap.elapsed.Truncate(time.Second))
}

func formatProgress(cfg benchConfig, elapsed time.Duration) string {
	if cfg.duration == 0 {
		return "manual stop"
	}

	total := cfg.warmup + cfg.duration
	if total <= 0 {
		return "complete"
	}

	ratio := float64(elapsed) / float64(total)
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	remaining := total - elapsed
	if remaining < 0 {
		remaining = 0
	}

	return fmt.Sprintf("%s %5.1f%%, remaining=%s", progressBar(ratio, 28), ratio*100, remaining.Truncate(time.Second))
}

func progressBar(ratio float64, width int) string {
	if width <= 0 {
		return ""
	}

	filled := int(math.Round(ratio * float64(width)))
	if filled < 0 {
		filled = 0
	}
	if filled > width {
		filled = width
	}

	return "[" + strings.Repeat("#", filled) + strings.Repeat(".", width-filled) + "]"
}

func errorRatio(requests, errors uint64) float64 {
	if requests == 0 {
		return 0
	}

	return float64(errors) / float64(requests)
}

func formatRatio(value float64) string {
	return fmt.Sprintf("%.2f%%", value*100)
}

func formatFloatTarget(rps float64) string {
	if rps == 0 {
		return "unlimited"
	}

	return fmt.Sprintf("%.1f", rps)
}

//nolint:lll // ok
func configHash(cfg benchConfig) string {
	data := fmt.Sprintf(
		"address=%s;connections=%d;warmup=%s;duration=%s;rps=%f;request_timeout=%s;idle_timeout=%s;max_message_size=%d;query=%s;key_prefix=%s;key_distribution=%s;key_start=%d;key_range=%d;batch=%d;seed=%d",
		cfg.address,
		cfg.connections,
		cfg.warmup,
		cfg.duration,
		cfg.rps,
		cfg.requestTimeout,
		cfg.idleTimeout,
		cfg.maxMessageSize,
		cfg.queryTemplate,
		cfg.keyPrefix,
		cfg.keyDistribution,
		cfg.keyStart,
		cfg.keyRange,
		cfg.batchSize,
		cfg.seed,
	)
	sum := sha256.Sum256([]byte(data))

	return hex.EncodeToString(sum[:8])
}

//nolint:revive,gocritic // ok
func max(a, b int) int {
	if a > b {
		return a
	}

	return b
}
