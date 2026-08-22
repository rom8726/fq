# fq

**fast quotas** - a small specialized database for frequency capping, rate limiting, and quota counters.

fq is built for a narrow set of high-throughput backend workloads where the main operation is to check or update counters inside time windows. It is not a general-purpose database and does not try to replace Redis, PostgreSQL, or other broad storage systems.

Use fq when you need:

- API rate limiting
- user, tenant, or token quotas
- ad frequency capping
- notification or message caps
- login, signup, or abuse throttling
- fast time-window counters with optional WAL, dumps, and async replication

## Features

- Atomic fixed-window rate limiter
- Atomic sliding-window rate limiter
- Atomic token-bucket rate limiter
- Counter commands for frequency capping
- In-memory storage engine
- WAL and periodic dumps for recovery
- Optional async master-slave replication
- Replica ack tracking and replication lag metrics
- Prometheus metrics and health endpoint
- CLI client and live benchmark client
- Go client: [fq-client-go](https://github.com/rom8726/fq-client-go)

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

### Counters

```text
INCR <key> <window>
GET <key> <window>
DEL <key> <window>
MDEL <key> <window> <key> <window> ...
WATCH <key> <window>
MSGSIZE
```

- `INCR`: increments the counter for a key inside a time window
- `GET`: returns the current counter value
- `DEL`: deletes counter and limiter state for the key/window pair
- `MDEL`: deletes multiple key/window pairs
- `WATCH`: waits until a key value changes or the request times out
- `MSGSIZE`: returns the maximum configured request/response payload size

Counter commands are useful for frequency capping and quota tracking where the application performs the decision itself.

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

Run a live latency/RPS benchmark against a running server:

```shell
make run-bench
```

Example with 500 connections for 60 seconds:

```shell
go run ./cmd/bench -address :1945 -connections 500 -duration 60s -key_range 10000
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
```

Endpoints:

- `GET /healthz`: liveness check
- `GET /metrics`: Prometheus metrics

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
