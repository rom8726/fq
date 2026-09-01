# Commands

This page is a usage guide with prose and examples. For the exact wire grammar,
response format, and error codes, see [Wire protocol](protocol.md).

## Rate Limiting

### Fixed Window

```text
RLIMIT FW <key> <limit> <window>
```

Example:

```text
RLIMIT FW user_42 100 60
```

This allows at most `100` requests for `user_42` in each fixed 60-second window.

### Sliding Window

```text
RLIMIT SW <key> <limit> <window>
```

Example:

```text
RLIMIT SW user_42 100 60
```

This allows at most `100` requests for `user_42` in the last 60 seconds.

### Token Bucket

```text
RLIMIT TB <key> <capacity> <refill_amount> <refill_window>
```

Example:

```text
RLIMIT TB user_42 100 10 60
```

This starts `user_42` with a bucket of `100` tokens. Each allowed request consumes one
token. Every 60 seconds the bucket receives up to `10` tokens, capped at `100`.

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

## Quotas

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

fq supports three quota ownership models:

- **Server-owned quotas**: `QUOTA SET` creates or updates quota `name` with `limit`.
  `QUOTA ACQ` then atomically reserves `amount` units for `client_id` without the
  client passing the limit. This is the preferred model when quota limits are central
  policy.
- **Server-decided quotas**: `QUOTA SETN` creates or updates quota `name` with `limit`
  and expected `clients`. `QUOTA ACQN` lets a client ask fq to assign its share; fq
  reserves up to `min(limit / clients, remaining)`.
- **Client-owned lease quotas**: `QUOTA ACQL` atomically reserves `amount` units from
  quota `name` for `client_id`, with the client passing `limit`. The first successful
  acquire creates the quota and fixes its `limit`; later acquires for the same quota
  must pass the same `limit`, otherwise fq returns an error.

`QUOTA SET` and `QUOTA SETN` return `ok|1` when the quota config was created or
changed and `ok|0` when it already had the same config. Lowering the limit below the
current active allocation total returns an error.

Quota ownership models cannot be mixed for the same quota name. A quota created with
`QUOTA SET` only accepts `QUOTA ACQ`, a quota created with `QUOTA SETN` only accepts
`QUOTA ACQN`, and a quota created with `QUOTA ACQL` only accepts `QUOTA ACQL`.

If `ttl` is provided, the client allocation expires and releases automatically after
that many seconds. `QUOTA REL` explicitly releases the allocation for one client.
`QUOTA DEL` deletes the whole quota only when it has no active client allocations.
`QUOTA INF` returns the current active allocations for a quota.

Repeated quota acquire calls from the same `client_id` are idempotent and return the
current allocation without extending its TTL. For `QUOTA ACQ` and `QUOTA ACQL`, a
repeated acquire with a different `amount` returns an error. For `QUOTA ACQN`, a
repeated acquire returns the existing allocation.

`QUOTA ACQ`, `QUOTA ACQN`, and `QUOTA ACQL` return:

```text
ok|<acquired>;<allocated>;<used>;<remaining>;<expires_after>
```

- `acquired`: `1` when the reservation exists after the command, `0` when there is not
  enough quota
- `allocated`: units reserved for this client by the command, or the existing
  idempotent reservation
- `used`: total active reserved units in the quota
- `remaining`: units still available
- `expires_after`: seconds until this client's allocation expires, or `0` for no TTL

`QUOTA REL` and `QUOTA DEL` return `ok|1` when state was removed and `ok|0` when there
was nothing to remove.

`QUOTA INF` returns:

```text
ok|<limit>;<used>;<remaining>[;<client_id>;<amount>;<expires_at>...]
```

Client fields are repeated in sorted `client_id` order. `expires_at` is a Unix
timestamp in seconds, or `0` for an allocation without TTL.

`QSTREAM` streams successful quota mutation events. `QPSTREAM` streams the same events
filtered to quota names that start with `prefix`.

Quota stream events return:

```text
ok|<event>;<name>;<client_id>;<amount>;<used>;<remaining>;<expires_at>
```

`event` is one of `acq`, `rel`, or `del`. `QUOTA SET` and `QUOTA SETN` do not emit
stream events. Idempotent quota acquire retries do not emit events because they do not
change state. For `del`, `client_id` is empty and the numeric fields are `0`.

### Examples

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

## Scanning Keys

```text
SCAN <cursor> <count>
PSCAN <prefix> <cursor> <count>
```

`SCAN` returns counter/rate-limit key/window pairs in chunks. `PSCAN` does the same,
filtered to keys that start with `prefix`. Start with cursor `0`; use the returned
cursor for the next request. A returned cursor of `0` means the scan is complete.

Key scanning requires `engine.key_index: true` (see [Configuration](config.md)). The
index is disabled by default to avoid extra write-path work for deployments that do
not need scanning. When it is disabled, `SCAN` and `PSCAN` return
`err|5000|scan index is disabled`.

Scan responses use:

```text
ok|<next_cursor>[;<key>;<window>...]
```

The cursor is opaque. Scan order is stable for existing keys, but scan is not a
snapshot: keys created or deleted during iteration may appear, disappear, or be seen
in a later full scan. Expired keys removed from in-memory state are skipped even if
their index entry has not been compacted yet.

## Database Maintenance

```text
FLUSHDB
TRUNCATE
```

- `FLUSHDB`: removes all in-memory keys, counters, limiters, and quotas. With dump
  enabled, fq removes the current dump snapshot; with WAL enabled, fq writes the flush
  LSN to `last_flushdb_lsn.meta`, so restart recovery ignores WAL entries at or before
  that point.
- `TRUNCATE`: removes all in-memory data and physically deletes dump and WAL files,
  including the `last_flushdb_lsn.meta` barrier.

Both commands return `ok|1`.

## Diagnostics

```text
INSPECT
INSPECT ALL
INSPECT WAL
INSPECT DUMP
INSPECT REPL
INSPECT ENGINE
INSPECT STREAMS
```

`INSPECT` returns a JSON snapshot of instance state for troubleshooting from the CLI,
without going through Prometheus. With no argument it returns a summary: instance
info, persistence config, and short aggregates for WAL, dump, replication, engine, and
streams, plus a computed `warnings` list (WAL queue pressure, replication lag, stale
replicas, a dump that hasn't run within its expected interval, stream subscribers
dropping events, and durability reminders for `sync_commit: off` or
`persistence.mode: memory`). `INSPECT ALL` returns the same shape without truncation
(full replica list, per-partition engine breakdown). A section name (`WAL`, `DUMP`,
`REPL`, `ENGINE`, `STREAMS`) returns just that section, untruncated, with no
`warnings`.

A field that does not apply to the current instance (for example `wal` fields on a
`dump_only` server, or `repl.slave` on a master) is `null` rather than a zero value.

Because a report can exceed one frame, `INSPECT` responses may span multiple frames
(see [Wire protocol](protocol.md) for the chunked-response grammar). The Go CLI
(`fq-cli`) and TCP client already implement reassembly.

`fq-cli` also accepts `HINSPECT` (with the same optional section argument, e.g.
`HINSPECT REPL`) as a client-side-only alias: it sends the equivalent `INSPECT` query
and renders the JSON as colored, tabular text instead of printing it raw. `HINSPECT`
is not a wire command — the server only ever sees `INSPECT`.

## Counters and full command index

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
HELLO <version> [AUTH <token>]
FLUSHDB
TRUNCATE
INSPECT [section]
AUTH <token>
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
- `STREAM`: streams limit-filled events as `ok|<key>;<window>;<current>;<reset_after>`
  frames
- `PSTREAM`: streams the same events, filtered to keys that start with `prefix`
- `QSTREAM`: streams quota mutation events
- `QPSTREAM`: streams the same quota events, filtered to quota names that start with
  `prefix`
- `HELLO`: negotiates the protocol version and reports the maximum payload size,
  whether authentication is required, and the connection's role — see
  [Wire protocol](protocol.md)
- `FLUSHDB`: clears all in-memory database state and persists a WAL recovery barrier
- `TRUNCATE`: clears all in-memory database state and deletes dump/WAL persistence
  files
- `INSPECT`: returns a JSON diagnostic snapshot of instance state; see Diagnostics
  above
- `AUTH`: authenticates the connection and assigns it a role; see
  [Operations](operations.md)

`STREAM` and `PSTREAM` emit an event when a rate-limit command moves a key/window from
below the limit to filled. Rejected rate-limit requests do not emit events. `current`
and `reset_after` match the `RLIMIT` result that filled the limit.

Clients should reconnect and resubscribe after idle disconnects. The Go TCP client
returns `network.ErrIdleTimeout` when its local idle deadline expires while waiting
for a frame; if the server closes the connection first, clients may receive `io.EOF`
or another connection-closed error instead.
