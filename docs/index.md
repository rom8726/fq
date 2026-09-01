# fq

**fq** is a small specialized database for frequency capping, rate limiting, and quota
counters.

fq is built for a narrow set of high-throughput backend workloads where the main
operation is to check or update counters inside time windows. It is not a
general-purpose database and does not try to replace Redis, PostgreSQL, or other broad
storage systems.

Use fq when you need:

- API rate limiting
- user, tenant, or token quotas
- shared resource lease quotas
- ad frequency capping
- notification or message caps
- login, signup, or abuse throttling
- fast time-window counters with optional WAL, dumps, and async replication

Source, releases, and the top-level project overview live at
[github.com/fq-db/fq](https://github.com/fq-db/fq).

## Where to start

| If you want to... | Read |
|---|---|
| Run your first command | [README quick start](https://github.com/fq-db/fq#quick-start) |
| See every command with examples | [Commands](commands.md) |
| Implement a client, or debug what a client sees on the wire | [Wire protocol](protocol.md) |
| Know what durability and isolation guarantees fq actually gives | [Consistency model](consistency.md) |
| Look up a configuration key | [Configuration](config.md) |
| Set up auth, TLS, replication, or monitoring in production | [Operations](operations.md) |
| Run or publish reproducible benchmark results | [Benchmarking](benchmarking.md) |
| Understand the on-disk format and recovery behavior | [Persistence](persistence.md) |

## Architecture at a glance

1. A client sends a framed TCP command.
2. The compute layer parses and validates the command.
3. Storage applies the operation to the in-memory engine.
4. Allowed write operations are recorded in WAL, depending on persistence mode.
5. Periodic dumps compact in-memory state into recoverable snapshots.
6. Replicas sync from a dump and then apply WAL chunks.

For what this actually promises under a crash or a network partition, see
[Consistency model](consistency.md).
