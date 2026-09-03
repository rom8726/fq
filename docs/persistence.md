# Persistence

This page covers the on-disk format and the WAL/dump machinery. For what durability
guarantees this actually buys you, see
[Consistency model](consistency.md#durability-logging-and-checkpoints).

Persistence is controlled by `persistence.mode`:

```yaml
persistence:
  mode: wal_and_dump # wal_and_dump | dump_only | memory
```

- `wal_and_dump`: write operations are stored in WAL and periodic dumps are created
- `dump_only`: periodic dumps are created, but write operations are not stored in WAL
- `memory`: data is kept only in memory, without WAL or dumps

Replication requires `wal_and_dump`, because replicas use the initial dump plus
continuous WAL replication.

## On-disk Format

WAL segments, dumps, and their `.meta` sidecars share one binary layout. Every file
starts with an 8-byte header:

```text
[magic 4B][version uint16 BE][reserved 2B]
```

Magic is `FQWL` for a WAL segment, `FQDP` for a dump, and `FQMT` for an LSN sidecar
(`wal_*.log.meta`, `last_flushdb_lsn.meta`). Each format is versioned independently.
Sidecars are always version 1. WAL segments and dumps are version 1 when their payloads
are stored as-is and version 2 when payloads carry a compression codec prefix. The
reserved bytes are written as zeros and ignored on read.

The header is followed by a stream of frames:

```text
[len uint32 BE][crc32c uint32 BE][payload len bytes]
```

`len` is the payload size, capped at 100 MB. A batch that would exceed the cap fails
the write instead of producing a file that cannot be read back. The CRC32C
(Castagnoli) checksum covers the length bytes and the payload together, so a corrupted
length field is detected directly instead of derailing the frame stream.

Reaction to a damaged file:

| Damage | Behavior |
|---|---|
| Incomplete trailing frame of the last WAL segment | Tail is truncated during recovery, startup continues |
| Checksum mismatch in any file | Startup fails, error names the file and frame offset |
| Foreign magic or unknown format version | Startup fails with the expected and actual format |
| Damaged `wal_*.log.meta` | Warning in the log, the segment is scanned instead of skipped |
| Damaged `last_flushdb_lsn.meta` | Startup fails |
| Damaged dump | Startup fails |

A zero-length WAL segment is treated as an empty segment and skipped: it means the
process died between creating the file and writing its header.

### Compression and format versions

In a version 2 file the frame payload is `[codec 1B][body]`. Codec `0` means the body is
the raw payload; codec `1` is s2 and codec `2` is zstd, and for those the body is
`[uncompressed size uvarint][compressed bytes]`. The frame header is unchanged, and `len`
and the CRC still describe the stored bytes, so frame scanning, torn-tail truncation, and
replication chunking behave exactly as they do for version 1 files.

Readers accept both versions. The default config enables `zstd`, so new WAL segments and
dumps are normally written as version 2. A file is written as version 2 only while the
matching [`compression`](config.md#compression) codec is enabled, so:

- turning a codec on affects files created from that point (WAL after the next segment
  rotation, dumps at the next snapshot); existing files keep working;
- turning it back off returns writes to version 1, and the version 2 files already on disk
  are still read by the same build.

Downgrading to a build that predates compression is only safe once no version 2 files
remain: such a build rejects the unknown format version at startup. Clear or regenerate
the WAL and dump directories first.

!!! warning "Upgrade note"
    Files written by builds without format headers are not readable. Clear the WAL
    and dump directories before upgrading from such a build.

## WAL Commit Mode

`wal.sync_commit` controls when a write command is acknowledged:

```yaml
wal:
  sync_commit: off # on | off
```

- `on`: the command waits until its WAL batch is written and synced to disk before the
  response is sent. This gives stronger durability, but response latency includes WAL
  batching and disk sync time.
- `off`: the command is applied to the in-memory engine and acknowledged without
  waiting for WAL sync. WAL is still written in the background, but a crash can lose
  commands that were already acknowledged and not flushed yet.

For quota and rate-limit workloads, `sync_commit: off` is often the better default:
losing a small recent slice of counters after a crash can be acceptable, while low
latency and high throughput are usually critical. Use `sync_commit: on` when
acknowledged writes must survive a process or machine crash.

## Stream Event Queue

`engine.limit_event_queue_capacity` controls the per-subscriber queue size for
`STREAM` events:

```yaml
engine:
  limit_event_queue_capacity: 16
```

If a stream subscriber is slower than incoming limit-filled events and its queue is
full, new events for that subscriber are dropped.

## Engine Partitions

`engine.partitions` controls how many independent in-memory hash table partitions are
used:

```yaml
engine:
  partitions: 16
```

Higher values reduce per-partition lock contention and make dump/clean snapshots
smaller, at the cost of more partition objects. If omitted or set to `0`, fq uses
`16`. See [Consistency model](consistency.md#concurrency-control) for what this buys
you concurrency-wise.

## WAL Apply Workers

`engine.wal_apply_workers` controls how many goroutines can apply one WAL chunk into
the in-memory engine:

```yaml
engine:
  wal_apply_workers: 4
```

Single-key WAL logs such as `INCR`, `DEL`, and rate-limit events are applied in their
original order inside the same in-memory partition, while different partitions can be
filled concurrently. `MDEL` is applied as a barrier between partition batches. The
replica sends ack only after the whole WAL chunk is applied. If omitted or set to `0`,
fq uses `1`.
