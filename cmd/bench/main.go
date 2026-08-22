package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/guptarohit/asciigraph"

	"fq/internal/network"
	"fq/internal/tools"
)

const defaultQueryTemplate = "INCR {key} {batch}"

type benchConfig struct {
	address        string
	connections    int
	duration       time.Duration
	rps            float64
	requestTimeout time.Duration
	idleTimeout    time.Duration
	maxMessageSize int
	queryTemplate  string
	keyPrefix      string
	keyStart       uint64
	keyRange       uint64
	batchSize      uint64
}

type result struct {
	latency time.Duration
	err     error
	badResp bool
	errText string
}

type snapshot struct {
	elapsed       time.Duration
	windowElapsed time.Duration
	windowCount   int
	windowErrors  int
	totalCount    uint64
	totalErrors   uint64
	latencies     []time.Duration
	lastError     string
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
		ctx, cancel = context.WithTimeout(ctx, cfg.duration)
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
	duration := flag.Duration("duration", 30*time.Second, "test duration; 0 means until Ctrl+C")
	rps := flag.Float64("rps", 0, "target total requests per second; 0 means unlimited")
	requestTimeout := flag.Duration("request_timeout", 5*time.Second, "timeout for one request")
	idleTimeout := flag.Duration("idle_timeout", time.Minute, "TCP connection idle timeout")
	maxMessageSizeStr := flag.String("max_message_size", "4KB", "max message size")
	queryTemplate := flag.String("query", defaultQueryTemplate, "query template; supports {key}, {batch}, {worker}, {n}")
	keyPrefix := flag.String("key_prefix", "bench", "key prefix for generated queries")
	keyStart := flag.Uint64("key_start", 0, "first generated key id")
	keyRange := flag.Uint64("key_range", 1000, "number of distinct generated keys")
	keys := flag.Uint64("keys", 1000, "deprecated alias for key_range")
	batchSize := flag.Uint64("batch", 600, "batch value for the default query template")
	flag.Parse()

	if *connections <= 0 {
		return benchConfig{}, fmt.Errorf("connections must be positive")
	}
	if *rps < 0 {
		return benchConfig{}, fmt.Errorf("rps must be non-negative")
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
		address:        *address,
		connections:    *connections,
		duration:       *duration,
		rps:            *rps,
		requestTimeout: *requestTimeout,
		idleTimeout:    *idleTimeout,
		maxMessageSize: maxMessageSize,
		queryTemplate:  *queryTemplate,
		keyPrefix:      *keyPrefix,
		keyStart:       *keyStart,
		keyRange:       *keyRange,
		batchSize:      *batchSize,
	}, nil
}

func runWorker(ctx context.Context, cfg benchConfig, workerID int, results chan<- result) {
	client, err := network.NewTCPClient(cfg.address, cfg.maxMessageSize, cfg.idleTimeout)
	if err != nil {
		sendResult(ctx, results, result{err: err})
		return
	}
	defer func() { _ = client.Close() }()

	var interval time.Duration
	var next time.Time
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

		query := makeQuery(cfg, workerID, n)
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

func makeQuery(cfg benchConfig, workerID int, n uint64) string {
	keyID := cfg.keyStart + (uint64(workerID)+n*uint64(cfg.connections))%cfg.keyRange
	replacer := strings.NewReplacer(
		"{key}", cfg.keyPrefix+"_"+strconv.FormatUint(keyID, 10),
		"{batch}", strconv.FormatUint(cfg.batchSize, 10),
		"{worker}", strconv.Itoa(workerID),
		"{n}", strconv.FormatUint(n, 10),
	)

	return replacer.Replace(cfg.queryTemplate)
}

func sendResult(ctx context.Context, results chan<- result, res result) {
	select {
	case results <- res:
	case <-ctx.Done():
	}
}

func runReporter(ctx context.Context, cfg benchConfig, results <-chan result, done <-chan struct{}) {
	start := time.Now()
	lastTick := start
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var totalCount uint64
	var totalErrors uint64
	var windowCount int
	var windowErrors int
	var lastError string
	windowLatencies := make([]time.Duration, 0, 65536)
	rpsHistory := make([]float64, 0, 60)
	p99History := make([]float64, 0, 60)

	for {
		select {
		case res := <-results:
			totalCount++
			windowCount++
			if res.err != nil || res.badResp {
				totalErrors++
				windowErrors++
				lastError = res.errText
			}
			if res.latency > 0 {
				windowLatencies = append(windowLatencies, res.latency)
			}
		case now := <-ticker.C:
			snap := snapshot{
				elapsed:       now.Sub(start),
				windowElapsed: now.Sub(lastTick),
				windowCount:   windowCount,
				windowErrors:  windowErrors,
				totalCount:    totalCount,
				totalErrors:   totalErrors,
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
			drainResults(results, &totalCount, &totalErrors, &windowCount, &windowErrors, &windowLatencies, &lastError)
			now := time.Now()
			snap := snapshot{
				elapsed:       now.Sub(start),
				windowElapsed: now.Sub(lastTick),
				windowCount:   windowCount,
				windowErrors:  windowErrors,
				totalCount:    totalCount,
				totalErrors:   totalErrors,
				latencies:     windowLatencies,
				lastError:     lastError,
			}
			renderSnapshot(cfg, snap, rpsHistory, p99History)
			fmt.Println("\nDone.")
			return
		case <-ctx.Done():
			<-done
		}
	}
}

func drainResults(
	results <-chan result,
	totalCount *uint64,
	totalErrors *uint64,
	windowCount *int,
	windowErrors *int,
	windowLatencies *[]time.Duration,
	lastError *string,
) {
	for {
		select {
		case res := <-results:
			*totalCount++
			*windowCount++
			if res.err != nil || res.badResp {
				*totalErrors++
				*windowErrors++
				*lastError = res.errText
			}
			if res.latency > 0 {
				*windowLatencies = append(*windowLatencies, res.latency)
			}
		default:
			return
		}
	}
}

func renderSnapshot(cfg benchConfig, snap snapshot, rpsHistory, p99History []float64) (rps, p99dur float64) {
	latencies := append([]time.Duration(nil), snap.latencies...)
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	rps = float64(snap.windowCount) / math.Max(snap.windowElapsed.Seconds(), 0.001)
	errRate := float64(snap.windowErrors) / math.Max(snap.windowElapsed.Seconds(), 0.001)
	p50 := percentile(latencies, 50)
	p95 := percentile(latencies, 95)
	p99 := percentile(latencies, 99)
	maxLatency := time.Duration(0)
	if len(latencies) > 0 {
		maxLatency = latencies[len(latencies)-1]
	}

	fmt.Print("\033[H\033[2J")
	fmt.Println("fq bench")
	fmt.Println(strings.Repeat("-", 72))
	fmt.Printf("address=%s  connections=%d  target_rps=%s  elapsed=%s\n",
		cfg.address,
		cfg.connections,
		formatTargetRPS(cfg.rps),
		snap.elapsed.Truncate(time.Second),
	)
	fmt.Printf("query=%q  key_range=[%d,%d)\n", cfg.queryTemplate, cfg.keyStart, cfg.keyStart+cfg.keyRange)
	fmt.Println(strings.Repeat("-", 72))
	fmt.Printf("now:   rps=%9.1f  errors/s=%7.1f  count=%8d  errors=%5d\n",
		rps,
		errRate,
		snap.windowCount,
		snap.windowErrors,
	)
	fmt.Printf("lat:   p50=%10s  p95=%10s  p99=%10s  max=%10s\n",
		formatDuration(p50),
		formatDuration(p95),
		formatDuration(p99),
		formatDuration(maxLatency),
	)
	fmt.Printf("total: requests=%d  errors=%d\n", snap.totalCount, snap.totalErrors)
	if snap.lastError != "" {
		fmt.Printf("last error: %s\n", truncate(snap.lastError, 120))
	}
	fmt.Println(strings.Repeat("-", 72))
	fmt.Println(renderGraph("RPS", appendHistory(rpsHistory, rps, 60)))
	fmt.Println(renderGraph("p99 latency, ms", appendHistory(p99History, durationMillis(p99), 60)))
	fmt.Println(strings.Repeat("-", 72))
	fmt.Println("Stop: Ctrl+C")

	return rps, durationMillis(p99)
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

//nolint:revive,gocritic // ok
func max(a, b int) int {
	if a > b {
		return a
	}

	return b
}
