# Changelog

All notable changes to fq will be documented in this file.

This project follows semantic versioning while it is pre-1.0: minor releases may
change behavior, and patch releases are reserved for compatible fixes.

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
