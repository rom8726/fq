# fq-stress

`fq-stress` runs failure-oriented stress scenarios against isolated fq server
processes.

Typical usage:

```shell
go run ./cmd/stress -scenario restart-smoke -duration 30s
go run ./cmd/stress -scenario crash-loop -duration 30s -workers 4 -keys 100 -kill_interval 2s -seed 42
go run ./cmd/stress -scenario dump-recovery -duration 30s -workers 4 -keys 100 -kill_interval 2s -dump_interval 250ms -seed 42
go run ./cmd/stress -scenario replication-stress -duration 30s -workers 4 -keys 100 -kill_interval 2s -sync_interval 100ms -seed 42
```

Each run writes a JSON report. Use `-report_file` for a stable output path and
`-keep_data` to keep generated WAL, dump, config, and log files after a successful run.
Failed runs keep their generated data automatically.

The nightly GitHub Actions stress workflow runs the crash-loop, dump-recovery, and
replication-stress scenarios and uploads their JSON reports as workflow artifacts.
