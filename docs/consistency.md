# Consistency model

This page states fq's guarantees explicitly, including what fq deliberately does
**not** provide.

## Data model

fq is a key/value store specialized for time-windowed counters, not a general-purpose
database. There are no rows, documents, or schemas. The unit of data is a
`(key, window)` pair holding one of:

- a counter value (`INCR`/`GET`/`DEL`),
- a rate-limit state (fixed window, sliding window, or token bucket),
- a quota allocation.

All state is in-memory; WAL and dumps exist for recovery, not as the primary store —
see [Persistence](persistence.md).

## Concurrency control

The in-memory engine is partitioned: keys are hashed across
`engine.partitions` independent hash tables (default 16; see
[Configuration](config.md)), each guarded by its own lock. A command touching a single
key/window pair is atomic with respect to other commands on the same key, because it
holds that partition's lock for the duration of the operation. Commands touching
different keys in different partitions proceed concurrently with no cross-partition
coordination.

There is no cross-key transaction mechanism. `MDEL`, which touches multiple
key/window pairs, is **not** atomic as a whole: each pair is deleted under its own
partition lock, so a concurrent reader can observe a state where some pairs are
already deleted and others are not yet.

## Isolation levels

fq has no multi-key transactions and therefore no isolation level in the traditional
sense. Isolation is scoped to a single command against a single key/window pair,
which is always linearizable with respect to other commands on that same pair. There
is no read isolation across multiple commands: two sequential `GET` calls on different
keys can observe writes from concurrent commands in either order.

## Durability (logging and checkpoints)

Durability is controlled by `persistence.mode` and, when WAL is in use,
`wal.sync_commit`:

| `persistence.mode` | WAL | Dumps | Durability |
|---|---|---|---|
| `wal_and_dump` | yes | yes | Full — see `sync_commit` below |
| `dump_only` | no | yes | Survives clean restarts (loads the last dump); loses everything written since the last dump on a crash |
| `memory` | no | no | Nothing survives a restart |

Within `wal_and_dump`, `wal.sync_commit` decides when a write is acknowledged:

- `sync_commit: on` — the response waits until the WAL batch containing the write is
  flushed and `fsync`ed. An acknowledged write survives a crash.
- `sync_commit: off` (the default) — the write is applied to memory and acknowledged
  immediately; the WAL record is flushed in the background. **A crash between
  acknowledgement and the next flush loses that write**, even though the client
  already received `ok|1` or a counter value reflecting it.

fq's default (`off`) trades this durability window for latency, on the assumption that
losing a small, recent slice of rate-limit or quota state after a crash is acceptable
for most of fq's target workloads. Set `sync_commit: on` when an acknowledged write
must survive a process or machine crash.

Checkpoints are periodic full dumps (`dump.interval`) plus WAL cleanup by LSN: once a
dump captures state up to some LSN, WAL segments entirely below that LSN are no longer
needed for recovery. See [Persistence](persistence.md) for the on-disk format and the
exact recovery procedure, including how a damaged file is handled.

## System architecture

A single fq deployment is **one master node plus zero or more async replicas** — there
is no clustering, no consensus protocol, and no automatic failover:

- The master is the only node that accepts writes.
- A replica pulls an initial dump, then continuously applies WAL chunks from the
  master (see [Operations](operations.md) for setup).
- Replication is asynchronous: the master acknowledges a write according to
  `wal.sync_commit` and does **not** wait for any replica to acknowledge it. A replica
  can lag behind the master by an amount visible via
  `fq_replication_lag_lsn`/`INSPECT REPL`.
- A replica serves reads, but those reads can be stale relative to the master by
  however far the replica currently lags.
- **There is no promote-to-master operation.** If the master is lost, nothing in fq
  turns a replica into the new master; that has to be done outside fq (reconfigure and
  restart a replica as `replica_type: master` against its own data, accepting that any
  WAL not yet applied from the old master is gone).
- There is no split-brain protection, because there is no mechanism by which two nodes
  could both become master automatically in the first place — promotion is manual, and
  only one node is ever configured as `replica_type: master` at a time.

This is the entire failure model: a master crash is a data-loss and availability event
until an operator intervenes, bounded by `sync_commit` on the write side and by
replication lag on the read side.
