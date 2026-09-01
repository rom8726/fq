# Benchmarking methodology

This page defines how fq benchmark results are produced, compared, and published.
Short ad hoc benchmark runs are useful during development, but only a reproducible
release run should be used as public performance evidence.

## Goals

Benchmark reports should answer four questions:

- what exact fq code was measured;
- what hardware and runtime were used;
- which workload profiles were executed;
- whether the result came from a complete, publishable run.

`cmd/results` is the source of truth for release result capture. It records the git
commit, dirty state, host label, operating system, architecture, Go version, CPU count,
selected environment details, config hashes, profile snapshots, the command manifest,
command logs, and generated JSON reports.

## Hardware

Published benchmark reports must name the machine class or host label used for the
run. Prefer a stable label passed with `-machine`:

```shell
go run ./cmd/results -mode release -machine dedicated-m2-pro -run -confirm_release_run
```

The generated metadata also records `hostname`, `goos`, `goarch`, `go_version`, and
`num_cpu`. Keep the hardware stable between reports when comparing versions. If the
machine changes, treat the new report as a new baseline rather than a direct
regression/progression measurement.

Before a publishable run:

- close unrelated CPU-heavy workloads;
- keep power and thermal conditions stable;
- avoid sharing the host with noisy jobs;
- record any unusual kernel, container, VM, or power-limit setting in the report text.

## Version

Published reports must identify the measured fq version by git commit. The results
tool writes:

- `git_commit`;
- `git_dirty`;
- `generated_at`;
- `repository_root`.

Use a clean working tree for public reports. A dirty run is still useful for local
debugging, but it should not be promoted as an official release baseline unless the
report explicitly describes the local changes.

## Server configuration

The default release run expects an fq server listening at `:1945`. Override it with
`-address` when measuring another instance:

```shell
go run ./cmd/results -mode release -address 127.0.0.1:1945 -run -confirm_release_run
```

`cmd/results` snapshots `config.yml`, `config-slave.yml`, `Makefile`, and every
benchmark profile into the run directory. It also records SHA-256 hashes for the
main config files. When publishing a report, include enough context to tell whether
the server used WAL, dumps, replication, TLS, authentication, and the intended
`engine.partitions`/`wal.sync_commit` settings.

## Profiles

Release benchmark runs use the profiles in `benchmarks/profiles`:

| Profile | Workload |
|---|---|
| `release-hot-counter.yml` | `INCR` against one hot key |
| `release-uniform-counter.yml` | `INCR` over a uniform key distribution |
| `release-fw.yml` | fixed-window `RLIMIT FW` over a Zipfian key distribution |
| `release-sw-uniform.yml` | sliding-window `RLIMIT SW` over a uniform key distribution |
| `release-sw-zipfian.yml` | sliding-window `RLIMIT SW` over a Zipfian key distribution |
| `release-tb.yml` | token-bucket `RLIMIT TB` over a Zipfian key distribution |

The smoke profile, `benchmarks/profiles/smoke.yml`, is for quick validation only. It
is not a publishable performance baseline.

Profile fields define the workload: connection count, warmup duration, measurement
duration, request timeout, idle timeout, initial max message size, query template,
key prefix, key distribution, key range, batch/window value, output format, output
path, and random seed. The release profiles currently use `seed: 42`, `duration: 60s`,
`warmup: 5s`, and `connections: 100`.

## Publishable run

A publishable run is a complete `release` results run:

```shell
go run ./cmd/results -mode release -run -confirm_release_run
```

It must include:

- all release benchmark profiles listed above;
- all release stress scenarios from the generated manifest;
- `metadata.json`;
- `manifest.json`;
- `summary.md`;
- benchmark JSON reports;
- stress JSON reports;
- command logs;
- config and profile snapshots.

The run directory is:

```text
benchmarks/results/runs/<timestamp>-<machine>-<commit>-release/
```

The command exits non-zero when any planned command fails. Failed runs are useful for
debugging, but they should be published only as failure evidence, not as performance
numbers.

## Stress scenarios

Release result capture also runs the failure-oriented stress scenarios:

| Scenario | Purpose |
|---|---|
| `crash-loop` | acknowledged writes survive repeated process kills and restarts |
| `dump-recovery` | dump creation and restart recovery preserve acknowledged state |
| `replication-stress` | slave restarts/reconnects and converges to master state |

These scenarios support the benchmark report by proving that the measured build still
survives the main failure modes. GitHub Actions also runs these stress scenarios
nightly and uploads their JSON reports as run artifacts.

## Reporting

Public reports live in `benchmarks/reports`. Generate one from a completed results
run with:

```shell
go run ./cmd/results -mode release -server_info_url http://db-host:2112/v1/info -run -confirm_release_run
go run ./cmd/report -input benchmarks/results/runs/<timestamp>-<machine>-<commit>-release/
```

By default, `cmd/report` writes `benchmarks/reports/report_YYYY_MM_DD.md` using the
current local date. It reads benchmark JSON reports from the run's `benchmarks/`
directory and, when present, includes `metadata.json`, `manifest.json`,
`server-info.json`, and stress JSON reports from `stress/`. Use `server_info_url`
when the benchmark client and database server run on different hosts.

A report should link or summarize the generated run directory and include:

- fq git commit and dirty state;
- machine label and hardware/runtime metadata;
- server configuration summary;
- list of profiles and stress scenarios;
- throughput and latency percentiles from each benchmark JSON report;
- error rate;
- notes about failures, retries, or environmental anomalies.

Do not compare results from different machines, different server configs, dirty
working trees, or partial profile sets as if they were one continuous benchmark series.
