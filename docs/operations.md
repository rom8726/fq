# Operations

Running fq in production: authentication, TLS, replication, monitoring, and the
benchmarking/stress tooling used to validate a release. For the meaning of every
config key referenced here, see [Configuration](config.md).

## Authentication

Both listening ports can require a shared secret, and both can be wrapped in TLS.

### Client port

Configure one token per role under `network.auth`. Secrets are never written inline in
YAML: each entry names either an environment variable or a file to read.

```yaml
network:
  address: ":1945"
  auth:
    tokens:
      - { role: admin, token_env: FQ_ADMIN_TOKEN }
      - { role: rw, token_env: FQ_RW_TOKEN }
      - { role: ro, token_file: /run/secrets/fq_ro_token }
```

A secret must be at least 16 characters after trimming surrounding whitespace, and no
two entries may resolve to the same value.

A connection must issue `HELLO <version> [AUTH <token>]` before any other command,
regardless of whether authentication is configured — see
[Wire protocol](protocol.md#handshake) for the handshake itself. Once at least one
token is configured, a role of `none` (no `AUTH`, or a failed one) cannot run anything
except `HELLO` and `AUTH`:

```text
[fq]> HELLO 1
ok|1;4096;1;none
[fq]> GET user_42 60
err|3000|not authenticated
[fq]> AUTH s3cret-admin-token-value
ok|1
[fq]> GET user_42 60
7
```

`AUTH` returns `ok|1` on success and `err|3002|authentication failed` on a wrong
token. Five failed attempts on one connection close it
(`err|3003|too many authentication failures`). The token is treated as an opaque
literal, so base64 values containing `=` or `+` work as-is, and it is never written to
the logs.

Roles are hierarchical — `admin` includes `rw`, and `rw` includes `ro`:

| Role | Commands |
|---|---|
| `ro` | `GET`, `SCAN`, `PSCAN`, `WATCH`, `STREAM`, `PSTREAM`, `QSTREAM`, `QPSTREAM`, `QUOTA INF` |
| `rw` | everything in `ro`, plus `INCR`, `DEL`, `MDEL`, `RLIMIT`, and the remaining `QUOTA` subcommands |
| `admin` | everything in `rw`, plus `FLUSHDB`, `TRUNCATE`, and `INSPECT` |

A command the current role does not cover returns `err|3001|permission denied`.

Leaving `network.auth` out entirely disables authentication on the client port and
logs a warning at startup. The port is then open to anyone who can reach it,
`FLUSHDB` and `TRUNCATE` included.

### Replication port

Replication has no unauthenticated mode. When `replication.replica_type` is set, a
secret is required, and the server refuses to start without one:

```yaml
replication:
  replica_type: master
  master_address: ":1946"
  sync_interval: 1s
  auth:
    token_env: FQ_REPLICATION_TOKEN
```

The master and its replicas share one secret. The replica sends it with every dump and
WAL request; the master compares it in constant time and rejects a mismatch with
replication error code `3002`, so a peer that cannot present the secret can neither
register as a replica nor pull the dump.

## TLS

Both ports accept an optional `tls` block. Setting `client_ca_file` on a server turns
on mutual TLS, requiring and verifying a client certificate.

```yaml
network:
  tls:
    cert_file: ./certs/server.crt
    key_file: ./certs/server.key
    client_ca_file: ./certs/ca.crt
    min_version: "1.3"

replication:
  tls:
    cert_file: ./certs/repl-server.crt
    key_file: ./certs/repl-server.key
    client_ca_file: ./certs/ca.crt
```

Field meaning by role is documented in [Configuration](config.md#tls-network-and-replication).

### Development certificates

`make certs` writes a local CA plus a server and a client certificate into `./certs`:

```shell
make certs
```

| File | Use |
|---|---|
| `ca.crt` | trust anchor: `ca_file` on clients, `client_ca_file` on servers |
| `server.crt` / `server.key` | server keypair for `network.tls` or `replication.tls` |
| `client.crt` / `client.key` | client keypair for mutual TLS |

The server certificate carries subject alternative names for `localhost`, `127.0.0.1`,
and `::1`. Override them when the server is reached under another name, and set the
validity window or output directory the same way:

```shell
CERT_HOSTS=fq.internal,10.0.0.7 CERT_DAYS=90 CERT_DIR=./certs make certs
```

The script refuses to overwrite an existing `server.crt`; `make certs-force` replaces
the whole set and `make certs-clean` removes it. `certs/` is in `.gitignore` and
`.dockerignore`, so private keys stay out of commits and out of the Docker build
context — generate certificates on the machine that needs them rather than committing
them. Container images ship an empty `/var/lib/fq/certs` for mounting real material:

```shell
docker run --rm \
  -p 1945:1945 -p 1946:1946 -p 2112:2112 \
  -e FQ_ADMIN_TOKEN -e FQ_REPLICATION_TOKEN \
  -v "$PWD/certs:/var/lib/fq/certs:ro" \
  -v "$PWD/config.yml:/etc/fq/config.yml:ro" \
  ghcr.io/fq-db/fq:latest
```

These certificates exist for local development and testing. Use certificates from
your own CA in production.

When TLS is enabled on the client port, local clients need trust settings of their
own. `fq -i` reads `network.tls.ca_file`, `server_name`, and `skip_verify`; if
`ca_file` is unset it falls back to trusting `network.tls.cert_file` directly, which
covers a self-signed certificate. The server certificate must cover the address
clients dial.

### Client flags

`fq-cli` and `fq-bench` take the same connection flags:

```shell
fq-cli -address :1945 -token "$FQ_ADMIN_TOKEN"

fq-cli -address fq.internal:1945 \
  -token "$FQ_ADMIN_TOKEN" \
  -tls_ca ./certs/ca.crt \
  -tls_cert ./certs/client.crt \
  -tls_key ./certs/client.key \
  -tls_server_name fq.internal
```

`-token` defaults to the `FQ_TOKEN` environment variable. `-tls_skip_verify` disables
server certificate verification and should stay off outside local testing.

Running `fq -i` connects the embedded CLI to its own port using an in-memory admin
token generated at startup, so the interactive mode needs no token of its own.

### Development tokens

The `run-*` and `bench-*` Makefile targets read fixed tokens declared at the top of
the `Makefile`, so `make run-server` and `make run-cli` work together out of the box:

```make
FQ_ADMIN_TOKEN ?= dev-admin-3f9a21c7b45e
FQ_RW_TOKEN ?= dev-rw-8c1d64e0a7b2
FQ_RO_TOKEN ?= dev-ro-5b7e03f9c8d1
FQ_SLAVE_ADMIN_TOKEN ?= dev-slave-admin-42a9c1
FQ_SLAVE_RO_TOKEN ?= dev-slave-ro-7e30b8d5
FQ_REPLICATION_TOKEN ?= dev-replication-9d24f6a1c3
```

Each is declared with `?=`, so an environment variable of the same name wins:

```shell
FQ_ADMIN_TOKEN=$(openssl rand -base64 32) make run-server
```

These values are committed to the repository and are therefore public. They exist so
the local dev loop runs without setup; generate real secrets for anything reachable by
others.

## Replication

Start a slave replica:

```shell
make run-slave
```

Or directly:

```shell
go run ./cmd/fq -c ./config-slave.yml
```

Master configuration:

```yaml
persistence:
  mode: wal_and_dump
replication:
  replica_type: master
  master_address: ":1946"
  sync_interval: 1s
  auth:
    token_env: FQ_REPLICATION_TOKEN
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
  auth:
    token_env: FQ_REPLICATION_TOKEN
```

Both nodes must resolve `replication.auth` to the same secret; the master rejects any
peer that does not present it with replication error code `3002`, and replication will
not start without a secret configured. For what "async" actually means for durability
and read staleness — and why there is no promote-to-master — see
[Consistency model](consistency.md#system-architecture).

## Monitoring

Health and metrics endpoints are enabled when `observability.address` is set:

```yaml
observability:
  address: ":2112"
  pprof: false
```

Endpoints:

- `GET /healthz` — liveness check
- `GET /metrics` — Prometheus metrics
- `GET /v1/info` — read-only JSON with instance/build/runtime/storage metadata

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

Available metrics:

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
- `fq_auth_failures_total{port="client"|"replication"}` — rejected authentication
  attempts on either port
- `fq_protocol_errors_total{code}` — error responses by numeric protocol error code,
  see [Wire protocol](protocol.md#error-codes)

For an ad hoc, non-Prometheus look at instance state, use `INSPECT` — see
[Commands](commands.md#diagnostics).

For remote benchmark publication, point `cmd/results` at the database server's
observability endpoint:

```shell
go run ./cmd/results -mode release -server_info_url http://db-host:2112/v1/info
```

The response is saved as `server-info.json` in the results run directory, so
`cmd/report` can show database-server metadata separately from benchmark-client
metadata. The raw JSON is intended as a local reproducibility artifact; published
Markdown reports should avoid private hostnames and IP addresses.

For authenticated or TLS-enabled benchmark targets, pass the same client options to
`cmd/results`; it forwards them to `cmd/bench`:

```shell
go run ./cmd/results -mode release \
  -address fq.internal:1945 \
  -token_env FQ_RW_TOKEN \
  -tls_ca ./certs/ca.crt \
  -tls_server_name fq.internal
```

## Benchmarking

Last benchmark reports: [benchmarks/reports](https://github.com/fq-db/fq/tree/main/benchmarks/reports).
For the reproducible release methodology, see [Benchmarking](benchmarking.md).

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

The benchmark screen updates once per second and shows current RPS, errors, latency
percentiles, and terminal history charts. Use `-key_range` to control how many
distinct keys are generated; smaller ranges create hotter keys and larger ranges
spread load across more keys.

For reproducible runs:

- `-profile` loads a YAML workload profile from `benchmarks/profiles`.
- `-warmup` excludes the warmup period from final metrics.
- `-output text|json|csv` controls the final report format.
- `-output_file` writes the final report to a file.
- `-seed` is recorded in report metadata and drives random key distributions.
- `-key_distribution sequential|uniform|zipfian` controls generated key selection.
- Final reports include p50, p95, p99, p99.9, max latency, throughput, error rate,
  Go/runtime metadata, and a benchmark config hash.

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

Run a replication stress scenario with master writes, slave reconnects, and
convergence verification:

```shell
go run ./cmd/stress -scenario replication-stress -duration 30s -workers 4 -keys 100 -kill_interval 2s -sync_interval 100ms -seed 42
```

The stress harness starts isolated fq server processes with temporary WAL/dump
directories, verifies readiness over TCP, writes data, kills and restarts processes,
then verifies recovery. In `crash-loop`, workers continue writing while the harness
repeatedly kills and restarts the server, and final verification checks that
acknowledged writes were not lost. In `dump-recovery`, the generated server config
uses a short dump interval, waits for a completed `current.dump`, restarts again, and
verifies recovery from persisted state. In `replication-stress`, workers write to the
master while the slave repeatedly restarts and reconnects; after writes stop, sampled
slave reads must converge to acknowledged master state.

Each run writes a JSON stress report with the scenario config, result summary,
generated config path, WAL/dump/log paths, expected counters, and the last stress
events. Use `-report_file` to write it to a stable path and `-keep_data` to keep the
generated stress directory after a successful run. Failed runs keep their generated
directory automatically.

The GitHub Actions `Stress` workflow runs `crash-loop`, `dump-recovery`, and
`replication-stress` nightly and uploads the JSON reports as run artifacts.

## Release results capture

Create a timestamped results directory with hardware/runtime metadata,
config/profile snapshots, and a command manifest without running heavy workloads:

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

Result runs are written to
`benchmarks/results/runs/<timestamp>-<machine>-<commit>-<mode>/`. Benchmark commands
expect a running fq server at `:1945` unless `-address` is overridden.

Render a publishable Markdown report from a completed results run:

```shell
go run ./cmd/report -input benchmarks/results/runs/<timestamp>-<machine>-<commit>-release/
```

By default the report is written to `benchmarks/reports/report_YYYY_MM_DD.md`.
