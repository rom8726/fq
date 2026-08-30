# fq

**fq** - a small specialized database for frequency capping, rate limiting, and quota counters.

fq is built for a narrow set of high-throughput backend workloads where the main operation is to check or update counters inside time windows. It is not a general-purpose database and does not try to replace Redis, PostgreSQL, or other broad storage systems.

Use fq when you need:

- API rate limiting
- user, tenant, or token quotas
- shared resource lease quotas
- ad frequency capping
- notification or message caps
- login, signup, or abuse throttling
- fast time-window counters with optional WAL, dumps, and async replication

See last benchmark reports: [benchmarks/reports](benchmarks/reports)

## Contents

- [Features](#features)
- [Installation](#installation)
- [Commands](#commands)
- [Quick Start](#quick-start)
- [Benchmarking](#benchmarking)
- [Stress testing](#stress-testing)
- [Release results capture](#release-results-capture)
- [Persistence](#persistence)
- [Replication](#replication)
- [Observability](#observability)
- [Architecture](#architecture)
- [Development](#development)

## Features

- Atomic fixed-window rate limiter
- Atomic sliding-window rate limiter
- Atomic token-bucket rate limiter
- Atomic server-owned and client-owned quota allocators with optional TTL
- Counter commands for frequency capping
- In-memory storage engine
- WAL and periodic dumps for recovery
- Optional async master-slave replication
- Replica ack tracking and replication lag metrics
- Prometheus metrics and health endpoint
- `INSPECT` diagnostic snapshot command (instance, WAL, dump, replication, engine, streams)
- CLI client and live benchmark client
- Go client: [fq-client-go](https://github.com/fq-db/fq-client-go)

## Installation

### Docker

```shell
docker run --rm \
  -p 1945:1945 \
  -p 1946:1946 \
  -p 2112:2112 \
  ghcr.io/fq-db/fq:latest
```

To use a local configuration file:

```shell
docker run --rm \
  -p 1945:1945 \
  -p 1946:1946 \
  -p 2112:2112 \
  -v "$PWD/config.yml:/app/config.yml:ro" \
  -v "$PWD/fq_data:/app/fq_data" \
  ghcr.io/fq-db/fq:latest
```

### Binaries

Prebuilt binaries for Linux and macOS are attached to each
[GitHub Release](https://github.com/fq-db/fq/releases). Release archives include:

- `fq` - server
- `fq-cli` - CLI client
- `fq-bench` - benchmark client
- example configuration files

### Debian Package

Linux releases also include `.deb` packages for amd64 and arm64. The package
installs:

- `fq` and `fq-cli` to `/usr/bin`
- the default config to `/etc/fq/config.yml`
- persistent WAL and dump storage under `/var/lib/fq`
- a systemd service named `fq.service`

Install and start fq:

```shell
sudo apt install ./fq_<version>_linux_<arch>.deb
sudo systemctl start fq
```

### From Source

```shell
git clone https://github.com/fq-db/fq.git
cd fq
make build
```

## Commands

fq uses a small text protocol over framed TCP requests.

### Rate Limiting

#### Fixed Window

```text
RLIMIT FW <key> <limit> <window>
```

Example:

```text
RLIMIT FW user_42 100 60
```

This allows at most `100` requests for `user_42` in each fixed 60-second window.

#### Sliding Window

```text
RLIMIT SW <key> <limit> <window>
```

Example:

```text
RLIMIT SW user_42 100 60
```

This allows at most `100` requests for `user_42` in the last 60 seconds.

#### Token Bucket

```text
RLIMIT TB <key> <capacity> <refill_amount> <refill_window>
```

Example:

```text
RLIMIT TB user_42 100 10 60
```

This starts `user_42` with a bucket of `100` tokens. Each allowed request consumes one token. Every 60 seconds the bucket receives up to `10` tokens, capped at `100`.

All rate-limit commands return:

```text
ok|<allowed>;<current>;<remaining>;<reset_after>
```

- `allowed`: `1` when the request is allowed, `0` when it is rejected
- `current`: current counter value for `FW`/`SW`; used bucket capacity for `TB`
- `remaining`: requests left before the limit is reached; tokens left for `TB`
- `reset_after`: seconds until capacity is available again

Example with limit `3`:

```text
[fq]> RLIMIT FW user_42 3 60
1;1;2;44
[fq]> RLIMIT FW user_42 3 60
1;2;1;43
[fq]> RLIMIT FW user_42 3 60
1;3;0;42
[fq]> RLIMIT FW user_42 3 60
0;3;0;41
```

Rejected rate-limit requests do not change state and are not written to WAL.

### Quotas

```text
QUOTA SET <name> <limit>
QUOTA SETN <name> <limit> <clients>
QUOTA ACQ <name> <amount> <client_id> [ttl]
QUOTA ACQN <name> <client_id> [ttl]
QUOTA ACQL <name> <limit> <amount> <client_id> [ttl]
QUOTA REL <name> <client_id>
QUOTA INF <name>
QUOTA DEL <name>
QSTREAM
QPSTREAM <prefix>
SCAN <cursor> <count>
PSCAN <prefix> <cursor> <count>
FLUSHDB
TRUNCATE
```

fq supports two quota ownership models:

- Server-owned quotas: `QUOTA SET` creates or updates quota `name` with `limit`.
  `QUOTA ACQ` then atomically reserves `amount` units for `client_id` without the
  client passing the limit. This is the preferred model when quota limits are
  central policy.
- Server-decided quotas: `QUOTA SETN` creates or updates quota `name` with
  `limit` and expected `clients`. `QUOTA ACQN` lets a client ask fq to assign its
  share; fq reserves up to `min(limit / clients, remaining)`.
- Client-owned lease quotas: `QUOTA ACQL` atomically reserves `amount` units from
  quota `name` for `client_id`, with the client passing `limit`. The first
  successful acquire creates the quota and fixes its `limit`; later acquires for
  the same quota must pass the same `limit`, otherwise fq returns an error.

`QUOTA SET` and `QUOTA SETN` return `ok|1` when the quota config was created or
changed and `ok|0` when it already had the same config. Lowering the limit below
the current active allocation total returns an error.

Quota ownership models cannot be mixed for the same quota name. A quota created
with `QUOTA SET` only accepts `QUOTA ACQ`, a quota created with `QUOTA SETN`
only accepts `QUOTA ACQN`, and a quota created with `QUOTA ACQL` only accepts
`QUOTA ACQL`.

If `ttl` is provided, the client allocation expires and releases automatically after
that many seconds. `QUOTA REL` explicitly releases the allocation for one client.
`QUOTA DEL` deletes the whole quota only when it has no active client allocations.
`QUOTA INF` returns the current active allocations for a quota.

Repeated quota acquire calls from the same `client_id` are idempotent and return
the current allocation without extending its TTL. For `QUOTA ACQ` and
`QUOTA ACQL`, a repeated acquire with a different `amount` returns an error.
For `QUOTA ACQN`, a repeated acquire returns the existing allocation.

`QUOTA ACQ`, `QUOTA ACQN`, and `QUOTA ACQL` return:

```text
ok|<acquired>;<allocated>;<used>;<remaining>;<expires_after>
```

- `acquired`: `1` when the reservation exists after the command, `0` when there is not enough quota
- `allocated`: units reserved for this client by the command, or the existing idempotent reservation
- `used`: total active reserved units in the quota
- `remaining`: units still available
- `expires_after`: seconds until this client's allocation expires, or `0` for no TTL

`QUOTA REL` and `QUOTA DEL` return `ok|1` when state was removed and `ok|0` when
there was nothing to remove.

`QUOTA INF` returns:

```text
ok|<limit>;<used>;<remaining>[;<client_id>;<amount>;<expires_at>...]
```

Client fields are repeated in sorted `client_id` order. `expires_at` is a Unix
timestamp in seconds, or `0` for an allocation without TTL.

`QSTREAM` streams successful quota mutation events. `QPSTREAM` streams the same
events filtered to quota names that start with `prefix`.

Quota stream events return:

```text
ok|<event>;<name>;<client_id>;<amount>;<used>;<remaining>;<expires_at>
```

`event` is one of `acq`, `rel`, or `del`. `QUOTA SET` and `QUOTA SETN` do not
emit stream events. Idempotent quota acquire retries do not
emit events because they do not change state. For `del`, `client_id` is empty and
the numeric fields are `0`.

### Scanning Keys

```text
SCAN <cursor> <count>
PSCAN <prefix> <cursor> <count>
```

`SCAN` returns counter/rate-limit key/window pairs in chunks. `PSCAN` does the
same, filtered to keys that start with `prefix`. Start with cursor `0`; use the
returned cursor for the next request. A returned cursor of `0` means the scan is
complete.

Key scanning requires `engine.key_index: true`. The index is disabled by default
to avoid extra write-path work for deployments that do not need scanning. When it
is disabled, `SCAN` and `PSCAN` return `err|scan index is disabled`.

Scan responses use:

```text
ok|<next_cursor>[;<key>;<window>...]
```

The cursor is opaque. Scan order is stable for existing keys, but scan is not a
snapshot: keys created or deleted during iteration may appear, disappear, or be
seen in a later full scan. Expired keys removed from in-memory state are skipped
even if their index entry has not been compacted yet.

Server-owned example with limit `10`:

```text
[fq]> QUOTA SET campaign_42 10
1
[fq]> QUOTA ACQ campaign_42 4 worker_a 60
1;4;4;6;60
[fq]> QUOTA ACQ campaign_42 7 worker_b
0;0;4;6;0
[fq]> QUOTA REL campaign_42 worker_a
1
[fq]> QUOTA DEL campaign_42
1
```

Server-decided example with limit `100000` and `20` expected clients:

```text
[fq]> QUOTA SETN service_rps 100000 20
1
[fq]> QUOTA ACQN service_rps worker_a 60
1;5000;5000;95000;60
[fq]> QUOTA ACQN service_rps worker_b
1;5000;10000;90000;0
```

Client-owned lease example with limit `10`:

```text
[fq]> QUOTA ACQL campaign_42 10 4 worker_a 60
1;4;4;6;60
[fq]> QUOTA INF campaign_42
10;4;6;worker_a;4;1788019260
[fq]> QUOTA ACQL campaign_42 10 4 worker_a 60
1;4;4;6;59
[fq]> QUOTA ACQL campaign_42 10 7 worker_b
0;0;4;6;0
[fq]> QUOTA REL campaign_42 worker_a
1
[fq]> QUOTA DEL campaign_42
1
```

### Database Maintenance

```text
FLUSHDB
TRUNCATE
```

- `FLUSHDB`: removes all in-memory keys, counters, limiters, and quotas. With dump
  enabled, fq removes the current dump snapshot; with WAL enabled, fq writes the
  flush LSN to `last_flushdb_lsn.meta`, so restart recovery ignores WAL entries
  at or before that point.
- `TRUNCATE`: removes all in-memory data and physically deletes dump and WAL files,
  including the `last_flushdb_lsn.meta` barrier.

Both commands return:

```text
ok|1
```

### Diagnostics

```text
INSPECT
INSPECT ALL
INSPECT WAL
INSPECT DUMP
INSPECT REPL
INSPECT ENGINE
INSPECT STREAMS
```

`INSPECT` returns a JSON snapshot of instance state for troubleshooting from the CLI, without going through Prometheus. With no argument it returns a summary: instance info, persistence config, and short aggregates for WAL, dump, replication, engine, and streams, plus a computed `warnings` list (WAL queue pressure, replication lag, stale replicas, a dump that hasn't run within its expected interval, stream subscribers dropping events, and durability reminders for `sync_commit: off` or `persistence.mode: memory`). `INSPECT ALL` returns the same shape without truncation (full replica list, per-partition engine breakdown). A section name (`WAL`, `DUMP`, `REPL`, `ENGINE`, `STREAMS`) returns just that section, untruncated, with no `warnings`.

A field that does not apply to the current instance (for example `wal` fields on a `dump_only` server, or `repl.slave` on a master) is `null` rather than a zero value.

Because a report can exceed one frame, `INSPECT` responses may span multiple frames:

```text
nxt|<partial JSON>
nxt|<partial JSON>
ok|<final partial JSON>
```

Clients concatenate the payloads of `nxt|` frames and the following terminal `ok|` (or `err|`) frame to reconstruct the full JSON document. A response that fits in one frame is returned directly as `ok|<json>`, so single-frame commands need no special handling. The Go CLI (`fq-cli`) and TCP client already implement this.

`fq-cli` also accepts `HINSPECT` (with the same optional section argument, e.g. `HINSPECT REPL`) as a client-side-only alias: it sends the equivalent `INSPECT` query and renders the JSON as colored, tabular text instead of printing it raw. `HINSPECT` is not a wire command — the server only ever sees `INSPECT`.

### Counters

```text
INCR <key> <window>
GET <key> <window>
DEL <key> <window>
MDEL <key> <window> <key> <window> ...
WATCH <key> <window>
SCAN <cursor> <count>
PSCAN <prefix> <cursor> <count>
QUOTA SET <name> <limit>
QUOTA SETN <name> <limit> <clients>
QUOTA ACQ <name> <amount> <client_id> [ttl]
QUOTA ACQN <name> <client_id> [ttl]
QUOTA ACQL <name> <limit> <amount> <client_id> [ttl]
QUOTA REL <name> <client_id>
QUOTA INF <name>
QUOTA DEL <name>
STREAM
PSTREAM <prefix>
QSTREAM
QPSTREAM <prefix>
MSGSIZE
FLUSHDB
TRUNCATE
INSPECT [section]
```

- `INCR`: increments the counter for a key inside a time window
- `GET`: returns the current counter value
- `DEL`: deletes counter and limiter state for the key/window pair
- `MDEL`: deletes multiple key/window pairs
- `WATCH`: waits until a key value changes or the request times out
- `SCAN`: returns key/window pairs in cursor-based chunks
- `PSCAN`: returns key/window pairs with a matching key prefix in cursor-based chunks
- `QUOTA SET`: creates or updates a server-owned quota limit
- `QUOTA ACQ`: reserves from a server-owned quota
- `QUOTA SETN`: creates or updates a server-decided quota split across clients
- `QUOTA ACQN`: reserves a server-decided per-client amount
- `QUOTA ACQL`: reserves from a client-owned lease quota
- `QUOTA REL`: releases one client's quota allocation
- `QUOTA INF`: returns active allocations for a quota
- `QUOTA DEL`: deletes an empty quota
- `STREAM`: streams limit-filled events as `ok|<key>;<window>;<current>;<reset_after>` frames
- `PSTREAM`: streams the same events, filtered to keys that start with `prefix`
- `QSTREAM`: streams quota mutation events
- `QPSTREAM`: streams the same quota events, filtered to quota names that start with `prefix`
- `MSGSIZE`: returns the maximum configured request/response payload size
- `FLUSHDB`: clears all in-memory database state and persists a WAL recovery barrier
- `TRUNCATE`: clears all in-memory database state and deletes dump/WAL persistence files
- `INSPECT`: returns a JSON diagnostic snapshot of instance state; see [Diagnostics](#diagnostics)

Counter commands are useful for frequency capping and quota tracking where the application performs the decision itself.

`STREAM` and `PSTREAM` emit an event when a rate-limit command moves a key/window from below the limit to filled. Rejected rate-limit requests do not emit events. `current` and `reset_after` match the `RLIMIT` result that filled the limit.

Clients should reconnect and resubscribe after idle disconnects. The Go TCP client returns `network.ErrIdleTimeout` when its local idle deadline expires while waiting for a frame; if the server closes the connection first, clients may receive `io.EOF` or another connection-closed error instead.

## Quick Start

Build binaries:

```shell
make build
```

This creates:

- `bin/fq` - server
- `bin/fq-cli` - CLI client
- `bin/fq-bench` - benchmark client

Run a master server:

```shell
make run-server
```

Connect with the CLI:

```shell
make run-cli
```

Try a fixed-window limiter:

```text
[fq]> RLIMIT FW user_42 3 60
1;1;2;44
```

Try a sliding-window limiter:

```text
[fq]> RLIMIT SW user_42 3 60
1;1;2;60
```

Try a token-bucket limiter:

```text
[fq]> RLIMIT TB user_42 10 1 60
1;1;9;0
```

## Benchmarking

Last benchmark reports: [benchmarks/reports](benchmarks/reports)

Run a live latency/RPS benchmark against a running server:

```shell
make run-bench
```

Example with 500 connections for 60 seconds:

```shell
go run ./cmd/bench -address :1945 -connections 500 -duration 60s -key_range 10000
```

Run a reproducible benchmark with warmup and a JSON report:

```shell
go run ./cmd/bench -profile benchmarks/profiles/smoke.yml
```

CLI flags can override profile values:

```shell
go run ./cmd/bench -profile benchmarks/profiles/release-fw.yml -connections 200 -duration 60s
```

Limit target load and customize generated keys:

```shell
go run ./cmd/bench -address :1945 -connections 200 -rps 50000 -key_range 100000 -batch 600
```

Benchmark a rate-limit command:

```shell
go run ./cmd/bench -address :1945 -connections 200 -duration 60s -query "RLIMIT FW {key} 100 {batch}"
```

Benchmark a token-bucket command:

```shell
go run ./cmd/bench -address :1945 -connections 200 -duration 60s -query "RLIMIT TB {key} 100 10 {batch}"
```

The benchmark screen updates once per second and shows current RPS, errors, latency percentiles, and terminal history charts. Use `-key_range` to control how many distinct keys are generated; smaller ranges create hotter keys and larger ranges spread load across more keys.

For reproducible runs:

- `-profile` loads a YAML workload profile from `benchmarks/profiles`.
- `-warmup` excludes the warmup period from final metrics.
- `-output text|json|csv` controls the final report format.
- `-output_file` writes the final report to a file.
- `-seed` is recorded in report metadata and drives random key distributions.
- `-key_distribution sequential|uniform|zipfian` controls generated key selection.
- Final reports include p50, p95, p99, p99.9, max latency, throughput, error rate, Go/runtime metadata, and a benchmark config hash.

Included profiles:

- `benchmarks/profiles/smoke.yml`
- `benchmarks/profiles/release-hot-counter.yml`
- `benchmarks/profiles/release-uniform-counter.yml`
- `benchmarks/profiles/release-fw.yml`
- `benchmarks/profiles/release-sw-uniform.yml`
- `benchmarks/profiles/release-sw-zipfian.yml`
- `benchmarks/profiles/release-tb.yml`

## Stress testing

Run the restart smoke stress scenario:

```shell
go run ./cmd/stress -scenario restart-smoke -duration 30s
```

Run a crash-loop stress scenario with concurrent writes:

```shell
go run ./cmd/stress -scenario crash-loop -duration 30s -workers 4 -keys 100 -kill_interval 2s -seed 42
```

Run a dump/recovery stress scenario with concurrent writes, dumps, and restarts:

```shell
go run ./cmd/stress -scenario dump-recovery -duration 30s -workers 4 -keys 100 -kill_interval 2s -dump_interval 250ms -seed 42
```

Run a replication stress scenario with master writes, slave reconnects, and convergence verification:

```shell
go run ./cmd/stress -scenario replication-stress -duration 30s -workers 4 -keys 100 -kill_interval 2s -sync_interval 100ms -seed 42
```

The stress harness starts isolated fq server processes with temporary WAL/dump directories, verifies readiness over TCP, writes data, kills and restarts processes, then verifies recovery. In `crash-loop`, workers continue writing while the harness repeatedly kills and restarts the server, and final verification checks that acknowledged writes were not lost. In `dump-recovery`, the generated server config uses a short dump interval, waits for a completed `current.dump`, restarts again, and verifies recovery from persisted state. In `replication-stress`, workers write to the master while the slave repeatedly restarts and reconnects; after writes stop, sampled slave reads must converge to acknowledged master state.

Each run writes a JSON stress report with the scenario config, result summary, generated config path, WAL/dump/log paths, expected counters, and the last stress events. Use `-report_file` to write it to a stable path and `-keep_data` to keep the generated stress directory after a successful run. Failed runs keep their generated directory automatically.

## Release results capture

Create a timestamped results directory with hardware/runtime metadata, config/profile snapshots, and a command manifest without running heavy workloads:

```shell
make results-plan
```

Run a quick local smoke capture for stress scenarios only:

```shell
make results-smoke
```

For publication runs on fixed hardware, execute the release manifest explicitly:

```shell
go run ./cmd/results -mode release -run -confirm_release_run
```

Result runs are written to `benchmarks/results/runs/<timestamp>-<machine>-<commit>-<mode>/`. Benchmark commands expect a running fq server at `:1945` unless `-address` is overridden.

## Persistence

Persistence is controlled by `persistence.mode`:

```yaml
persistence:
  mode: wal_and_dump # wal_and_dump | dump_only | memory
```

- `wal_and_dump`: write operations are stored in WAL and periodic dumps are created
- `dump_only`: periodic dumps are created, but write operations are not stored in WAL
- `memory`: data is kept only in memory, without WAL or dumps

Replication requires `wal_and_dump`, because replicas use the initial dump plus continuous WAL replication.

### WAL Commit Mode

`wal.sync_commit` controls when a write command is acknowledged:

```yaml
wal:
  sync_commit: off # on | off
```

- `on`: the command waits until its WAL batch is written and synced to disk before the response is sent. This gives stronger durability, but response latency includes WAL batching and disk sync time.
- `off`: the command is applied to the in-memory engine and acknowledged without waiting for WAL sync. WAL is still written in the background, but a crash can lose commands that were already acknowledged and not flushed yet.

For quota and rate-limit workloads, `sync_commit: off` is often the better default: losing a small recent slice of counters after a crash can be acceptable, while low latency and high throughput are usually critical. Use `sync_commit: on` when acknowledged writes must survive a process or machine crash.

### Stream Event Queue

`engine.limit_event_queue_capacity` controls the per-subscriber queue size for `STREAM` events:

```yaml
engine:
  limit_event_queue_capacity: 16
```

If a stream subscriber is slower than incoming limit-filled events and its queue is full, new events for that subscriber are dropped.

### Engine Partitions

`engine.partitions` controls how many independent in-memory hash table partitions are used:

```yaml
engine:
  partitions: 10
```

Higher values reduce per-partition lock contention and make dump/clean snapshots smaller, at the cost of more partition objects. If omitted or set to `0`, fq uses `10`.

### WAL Apply Workers

`engine.wal_apply_workers` controls how many goroutines can apply one WAL chunk into the in-memory engine:

```yaml
engine:
  wal_apply_workers: 4
```

Single-key WAL logs such as `INCR`, `DEL`, and rate-limit events are applied in their original order inside the same in-memory partition, while different partitions can be filled concurrently. `MDEL` is applied as a barrier between partition batches. The replica sends ack only after the whole WAL chunk is applied. If omitted or set to `0`, fq uses `1`.

## Replication

fq supports async master-slave replication:

- Master accepts writes and serves WAL chunks to replicas
- Slave synchronizes an initial dump, then continuously applies WAL updates
- Slave sends ack progress with `replica_id`, WAL cursor, and last applied LSN
- Master tracks known replicas and exposes ack/lag metrics
- Slave reconnects automatically with exponential backoff

Start a slave replica:

```shell
make run-slave
```

Or directly:

```shell
go run ./cmd/fq config-slave.yml
```

Master configuration:

```yaml
persistence:
  mode: wal_and_dump
replication:
  replica_type: master
  master_address: ":1946"
  sync_interval: 1s
```

Slave configuration:

```yaml
persistence:
  mode: wal_and_dump
replication:
  replica_type: slave
  replica_id: "replica-1"
  master_address: ":1946"
  sync_interval: 1s
```

Replication is currently async. Writes are acknowledged by the master according to `wal.sync_commit`; they do not wait for replica acknowledgement.

## Observability

Health and metrics endpoints are enabled when `observability.address` is set:

```yaml
observability:
  address: ":2112"
  pprof: false
```

Endpoints:

- `GET /healthz`: liveness check
- `GET /metrics`: Prometheus metrics

CPU/heap profiling can be enabled explicitly:

```yaml
observability:
  address: "127.0.0.1:2112"
  pprof: true
```

Then capture profiles during a benchmark run:

```shell
go tool pprof "http://127.0.0.1:2112/debug/pprof/profile?seconds=30"
go tool pprof "http://127.0.0.1:2112/debug/pprof/heap"
```

Available metrics include:

- `fq_tcp_active_connections`
- `fq_wal_queue_depth`
- `fq_wal_flush_duration_seconds`
- `fq_wal_flush_batch_records`
- `fq_wal_flush_total`
- `fq_replication_lag_lsn`
- `fq_replication_reconnect_total`
- `fq_replication_reconnect_attempts_total`
- `fq_replication_replica_last_applied_lsn{replica_id}`
- `fq_replication_replica_last_ack_timestamp{replica_id}`
- `fq_replication_known_replicas`

## Architecture

fq is intentionally small:

1. Client sends a framed TCP command.
2. Compute layer parses and validates the command.
3. Storage applies the operation to the in-memory engine.
4. Allowed write operations are recorded in WAL depending on persistence mode.
5. Periodic dumps compact in-memory state into recoverable snapshots.
6. Replicas sync from dump and then apply WAL chunks.

Rate-limit commands are atomic per key/window. For `RLIMIT FW`, allowed requests are stored as counter increments. For `RLIMIT SW`, allowed requests are stored as sliding-window events. For `RLIMIT TB`, allowed requests are stored as token-bucket consume events with their capacity and refill parameters.

## Development

Run tests:

```shell
make test
```

Run race tests:

```shell
make test-race
```

Run linter:

```shell
make lint
```

Build WAL protocol buffers:

```shell
make proto.wal.build
```
