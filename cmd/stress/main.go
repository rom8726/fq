package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/fq-db/fq/internal/stress"
)

//nolint:lll // ok
func main() {
	opts := stress.Options{}
	flag.StringVar(&opts.Scenario, "scenario", stress.RestartSmokeScenario, "stress scenario")
	flag.DurationVar(&opts.Duration, "duration", time.Minute, "stress duration reserved for longer scenarios")
	flag.Int64Var(&opts.Seed, "seed", 42, "stress seed")
	flag.IntVar(&opts.Workers, "workers", 4, "number of stress workers")
	flag.IntVar(&opts.Keys, "keys", 100, "number of generated counter keys")
	flag.DurationVar(&opts.KillInterval, "kill_interval", 2*time.Second, "interval between crash-loop kills")
	flag.DurationVar(&opts.DumpInterval, "dump_interval", 0, "dump interval for dump-recovery; 0 chooses scenario default")
	flag.DurationVar(&opts.RequestTimeout, "request_timeout", time.Second, "timeout for one verification request")
	flag.DurationVar(&opts.SyncInterval, "sync_interval", 0, "replication sync interval; 0 chooses scenario default")
	flag.StringVar(&opts.ReportFile, "report_file", "", "stress JSON report path; empty means stress-result.json in workdir")
	flag.BoolVar(&opts.KeepData, "keep_data", false, "keep generated stress data directory")
	flag.StringVar(&opts.WorkDir, "workdir", "", "stress work directory; empty means temp dir")
	flag.StringVar(&opts.FQBinary, "fq_binary", "", "fq server binary; empty means go run ./cmd/fq")
	flag.StringVar(&opts.RepositoryDir, "repo", ".", "repository directory used when fq_binary is empty")
	flag.Parse()

	result, err := stress.Run(context.Background(), opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "stress failed:", err)
		os.Exit(1)
	}

	fmt.Printf(
		"stress ok: scenario=%s address=%s slave_address=%s operations=%d restarts=%d dumps=%d transient_errors=%d\n",
		result.Scenario,
		result.Address,
		result.SlaveAddress,
		result.Operations,
		result.Restarts,
		result.Dumps,
		result.TransientErrors,
	)
	if opts.KeepData {
		fmt.Printf("stress data: %s\n", result.RootDir)
	}
	if opts.ReportFile != "" || opts.KeepData {
		fmt.Printf("stress report: %s\n", result.ReportPath)
	}
}
