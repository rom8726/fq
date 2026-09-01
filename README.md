# fq

[![CI](https://github.com/fq-db/fq/actions/workflows/makefile.yml/badge.svg?branch=main)](https://github.com/fq-db/fq/actions/workflows/makefile.yml)
[![Coverage Status](https://coveralls.io/repos/github/fq-db/fq/badge.svg?branch=main)](https://coveralls.io/github/fq-db/fq?branch=main)
[![Docs](https://github.com/fq-db/fq/actions/workflows/docs.yml/badge.svg?branch=main)](https://github.com/fq-db/fq/actions/workflows/docs.yml)
[![Stress](https://github.com/fq-db/fq/actions/workflows/stress.yml/badge.svg?branch=main)](https://github.com/fq-db/fq/actions/workflows/stress.yml)
[![Release](https://img.shields.io/github/v/release/fq-db/fq?sort=semver)](https://github.com/fq-db/fq/releases)
[![Go Reference](https://pkg.go.dev/badge/github.com/fq-db/fq.svg)](https://pkg.go.dev/github.com/fq-db/fq)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

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

📚 **Full documentation:** https://fq-db.github.io/fq/ — commands with examples, the
wire protocol, the consistency model, the configuration reference, and the operations
guide (auth, TLS, replication, monitoring).

## Contents

- [Features](#features)
- [Installation](#installation)
- [Commands](#commands)
- [Security](#security)
- [Quick Start](#quick-start)
- [Persistence](#persistence)
- [Replication](#replication)
- [Observability](#observability)
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
  -e FQ_ADMIN_TOKEN -e FQ_RW_TOKEN -e FQ_RO_TOKEN -e FQ_REPLICATION_TOKEN \
  ghcr.io/fq-db/fq:latest
```

To use a local configuration file:

```shell
docker run --rm \
  -p 1945:1945 \
  -p 1946:1946 \
  -p 2112:2112 \
  -e FQ_ADMIN_TOKEN -e FQ_RW_TOKEN -e FQ_RO_TOKEN -e FQ_REPLICATION_TOKEN \
  -v "$PWD/config.yml:/etc/fq/config.yml:ro" \
  -v "$PWD/fq_data:/var/lib/fq/fq_data" \
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

fq uses a small text protocol over framed TCP requests: `HELLO` to negotiate the
protocol version, then commands like `INCR`, `RLIMIT`, and `QUOTA`. Every command
returns `ok|...` or `err|<code>|<message>`.

```text
[fq]> HELLO 1
ok|1;4096;0;admin
[fq]> RLIMIT FW user_42 3 60
1;1;2;44
```

Full command reference with examples: **[Commands](https://fq-db.github.io/fq/commands/)**.
Wire grammar, handshake, and the error code table: **[Wire protocol](https://fq-db.github.io/fq/protocol/)**.

## Security

Both listening ports can require a shared secret (`network.auth` /
`replication.auth`), and both can be wrapped in TLS. A connection authenticates with
`HELLO <version> [AUTH <token>]`; roles are hierarchical (`admin` ⊇ `rw` ⊇ `ro`) and
gate which commands a connection can run.

```text
[fq]> HELLO 1
ok|1;4096;1;none
[fq]> GET user_42 60
err|3000|not authenticated
[fq]> AUTH s3cret-admin-token-value
ok|1
```

Setting up tokens, TLS, development certificates, and the client-side flags: see
**[Operations](https://fq-db.github.io/fq/operations/)**. Every config key involved:
see **[Configuration](https://fq-db.github.io/fq/config/)**.

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

Or start the server with the built-in interactive TUI:

```shell
make run-interactive
# or: bin/fq -i -c ./config.yml
```

Or run the interactive TUI from Docker:

```shell
make docker-run-interactive
# or:
docker run --rm -it \
  --entrypoint /var/lib/fq/fq \
  -p 1945:1945 \
  -p 1946:1946 \
  -p 2112:2112 \
  -e FQ_ADMIN_TOKEN=dev-admin-3f9a21c7b45e \
  -e FQ_RW_TOKEN=dev-rw-8c1d64e0a7b2 \
  -e FQ_RO_TOKEN=dev-ro-5b7e03f9c8d1 \
  -e FQ_REPLICATION_TOKEN=dev-replication-9d24f6a1c3 \
  ghcr.io/fq-db/fq:latest -i
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

Benchmarking, stress testing, and reproducible release captures are covered in
**[Operations](https://fq-db.github.io/fq/operations/)**.

## Persistence

Persistence is controlled by `persistence.mode`: `wal_and_dump` (WAL + periodic
dumps), `dump_only` (dumps only), or `memory` (no persistence). Replication requires
`wal_and_dump`.

On-disk format, WAL commit modes, and recovery behavior for a damaged file: see
**[Persistence](https://fq-db.github.io/fq/persistence/)**. What each mode actually
guarantees under a crash: see
**[Consistency model](https://fq-db.github.io/fq/consistency/)**.

## Replication

fq supports async master-slave replication: a master accepts writes and serves WAL
chunks; a slave pulls an initial dump, then continuously applies WAL updates and
reconnects automatically with exponential backoff.

```shell
make run-slave
# or: go run ./cmd/fq -c ./config-slave.yml
```

Replication is asynchronous — writes are acknowledged by the master according to
`wal.sync_commit` and do not wait for replica acknowledgement, and there is no
automatic failover. See **[Consistency model](https://fq-db.github.io/fq/consistency/#system-architecture)**
for what that means in practice, and **[Operations](https://fq-db.github.io/fq/operations/#replication)**
for setup.

## Observability

Health and metrics endpoints are enabled when `observability.address` is set:

```yaml
observability:
  address: ":2112"
```

- `GET /healthz` — liveness check
- `GET /metrics` — Prometheus metrics (connections, WAL flush latency, replication
  lag, auth failures, protocol error codes, and more)
- `GET /v1/info` — read-only JSON with instance/build/runtime/storage metadata for
  benchmark reports

Metric list, `INSPECT` usage, and pprof profiling: see
**[Operations](https://fq-db.github.io/fq/operations/#monitoring)**.

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

Serve the documentation site locally:

```shell
pip install -r docs/requirements.txt
mkdocs serve
```
