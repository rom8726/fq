# Changelog

All notable changes to fq will be documented in this file.

This project follows semantic versioning while it is pre-1.0: minor releases may
change behavior, and patch releases are reserved for compatible fixes.

## [v0.10.1]

### Changed

- Dump files now start with a checkpoint record holding the exact transaction the
  snapshot was taken at, and restore uses it as the WAL replay point instead of the
  highest transaction seen among the dumped keys. This makes the restore point match
  the LSN already used for WAL cleanup.
- Replicas reject write commands with the new error code `5005` instead of applying
  them locally. A replica has no running WAL writer, so such a write was never
  persisted, diverged from the master, and stalled the connection once the WAL queue
  filled. Read commands, `QUOTA INF` and `INSPECT` are still served.
- A replica holds off its first periodic dump until the initial dump synchronization
  from the master has been fully applied. Snapshotting midway through it would capture
  a mix of keys that is consistent at no transaction at all.
- Taking a dump snapshot no longer streams elements through a channel one at a time.
  Each partition is now snapshotted in parallel into a preallocated slice, which cuts
  the window during which writes are blocked by roughly an order of magnitude (about
  200ms to about 20ms for one million keys on sixteen partitions).
- `FLUSHDB` and `TRUNCATE` no longer hold the mutation lock while waiting for the
  dumper, so a flush issued during a long dump no longer blocks every writer for the
  duration of that dump.

### Fixed

- Fixed dumps losing acknowledged writes under concurrent load, which could roll a
  counter back to a stale value after recovery. Keys written more than once during a
  dump had their saved pre-image overwritten with a post-snapshot value and were then
  dropped from the dump entirely, and token buckets and quota allocations never had a
  pre-image at all. Choosing the dump transaction and reading the engine snapshot now
  happen under a short write barrier; writing, compressing and renaming the file still
  run without blocking writers.
- Fixed a replica never reconnecting after its connection to the master broke. Every
  non-timeout transport failure, including a broken pipe, a reset connection and a
  closed connection, was classified as an application error, so the replica kept
  writing to a dead socket until it stopped replicating altogether.
- Fixed a replica pausing for five minutes without honouring shutdown or context
  cancellation after reaching the retry limit, which also hung `Shutdown` for the same
  period.
- Fixed the dump synchronization path retrying without any delay, so a persistent
  failure hit the retry limit almost immediately. Both synchronization paths now share
  the same exponential backoff, while a healthy dump transfer still proceeds batch
  after batch without added delay.
- Fixed a replica's periodic dump omitting every key modified since the process
  started and stamping the file with the transaction of that startup, so restoring
  from it dropped those keys' earlier increments. A replica's transaction counter does
  not advance with replicated writes, so the dump was taken at a frozen point for
  which no key touched since had a snapshot. The engine now tracks the highest
  transaction it has applied and takes the snapshot under a short gate over the WAL
  and dump apply paths, and the checkpoint is written from that. WAL cleanup on a
  replica works again as a result, so its local WAL no longer grows without bound and
  a restart no longer replays every segment from the beginning.
- Fixed a replica losing the dump checkpoint LSN when later dump batches carried lower
  transaction numbers.

### Tests

- Widened the counter window used by the stress scenarios so a run crossing a
  fixed-window boundary no longer resets the counters it is verifying.

## [v0.10.0]

### Added

- Added s2/zstd compression for WAL segments, dump files, and replication traffic,
  configured in the new `compression` config section. The default config now uses
  `zstd` for WAL, dumps, and replication; set a codec to `none` to disable it.
  Compressed files use format version 2; readers accept both versions, and a master with
  compressed WAL segments refuses to serve a replica that does not support the codec
  (error code `5004`) instead of sending bytes it cannot read.

## [v0.9.1]

### Added

- Ensure WAL directory creation and handle missing directory creation in readLogs test.

## [v0.9.0]

### Added

- Added Prometheus/Grafana observability assets, including a Docker Compose stack,
  Prometheus scrape config, Grafana datasource provisioning, and an fq overview
  dashboard.
- Added the `/v1/info` observability endpoint and wired benchmark result capture to
  include server metadata from it.
- Added benchmarking documentation and READMEs for the benchmark, report, results,
  and stress helper commands.
- Added support for authenticated and TLS-enabled benchmark targets.
- Added progress output and benchmark profile validation for release result runs.
- Added periodic retry notifications in the interactive TUI while waiting for the
  server connection.
- Added a Helm chart for Kubernetes deployments, including generated config,
  Secret-file or environment-based tokens, persistent storage, probes, Service, and
  optional ServiceMonitor.
- Added repository hygiene files: security policy, contribution guide, code of
  conduct, issue templates, pull request template, and code owners.

### Changed

- Improved benchmark reporting and release result capture output.
- Allowed release benchmark profiles to override the target address explicitly.
- Redacted private hostnames and IP addresses from generated reports.
- Clarified why the default client frame size is `4KB` and how chunked `INSPECT`
  responses use `nxt|` frames.
- Pinned Docker base images by digest and aligned Docker build images with Go
  `1.26.8`.
- Excluded command binaries from coverage reporting.
- Refactored dump frame construction around a shared frame-header helper.
- Reordered initializer goroutines for more consistent startup behavior.

### Fixed

- Fixed replica WAL recovery by applying replicated increments during recovery.
- Ensured WAL recovery waits for engine apply before continuing.

### Tests

- Added fuzz coverage for the command parser and network frame reader.
- Expanded unit coverage for inspect, storage, observability, and shared tooling.

## Previous Releases

Changes before `v0.9.0` were not tracked in this changelog.
