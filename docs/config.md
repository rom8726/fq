# Configuration

fq reads a single YAML file, given with `-c` (default `./config.yml`). This page
documents every key. The repository ships two working examples:
[`config.yml`](https://github.com/fq-db/fq/blob/main/config.yml) (master) and
[`config-slave.yml`](https://github.com/fq-db/fq/blob/main/config-slave.yml) (slave
replica).

Unknown keys are rejected at load time (`yaml.Decoder.KnownFields(true)`), and the
whole file is validated before the server starts — a bad value fails fast with a
message naming the offending section.

Sizes (`max_message_size`, `max_segment_size`) accept a plain byte count or a suffix:
`KB`/`Kb`/`kb`, `MB`/`Mb`/`mb`, `GB`/`Gb`/`gb` — binary multiples (`1MB` = 1,048,576
bytes).

## `network`

The client port.

```yaml
network:
  address: ":1945"
  max_connections: 1000
  max_message_size: 4096
  idle_timeout: 10m
  auth:
    tokens:
      - { role: admin, token_env: FQ_ADMIN_TOKEN }
  tls:
    cert_file: ./certs/server.crt
    key_file: ./certs/server.key
```

| Key | Type | Required | Notes |
|---|---|---|---|
| `address` | `host:port` | yes | Listen address for client connections |
| `max_connections` | int ≥ 1 | yes | Maximum concurrent client connections |
| `max_message_size` | size | yes | Maximum request/response frame payload; reported to clients in the `HELLO` response |
| `idle_timeout` | duration | yes | Connection is closed after this much inactivity |
| `auth` | [`AuthConfig`](#networkauth) | no | Omit entirely to disable authentication (logs a startup warning) |
| `tls` | [`TLSConfig`](#tls-network-and-replication) | no | Omit to serve plaintext |

### `network.auth`

```yaml
auth:
  tokens:
    - { role: admin, token_env: FQ_ADMIN_TOKEN }
    - { role: rw, token_env: FQ_RW_TOKEN }
    - { role: ro, token_file: /run/secrets/fq_ro_token }
```

One entry per role. `role` is one of `admin`, `rw`, `ro`. Exactly one of `token_env` or
`token_file` must be set per entry — never both, never neither. A secret must be at
least 16 characters after trimming whitespace, and no two entries may resolve to the
same value. See [Operations](operations.md) for the full authentication walkthrough
and the role/command matrix.

## `persistence`

```yaml
persistence:
  mode: wal_and_dump # wal_and_dump | dump_only | memory
```

| Key | Type | Required | Notes |
|---|---|---|---|
| `mode` | enum | no, defaults to `wal_and_dump` | `wal_and_dump`, `dump_only`, or `memory` — see [Consistency model](consistency.md#durability-logging-and-checkpoints) |

Replication requires `wal_and_dump`; the server refuses to start replication under any
other mode.

## `wal`

Required (and validated) when `persistence.mode` is `wal_and_dump`; omitted or ignored
otherwise.

```yaml
wal:
  sync_commit: off # on | off
  flushing_batch_length: 8192
  flushing_batch_timeout: 10ms
  queue_capacity: 16384
  max_segment_size: 64MB
  data_directory: ./fq_data/wal/
```

| Key | Type | Required | Notes |
|---|---|---|---|
| `sync_commit` | `on` \| `off` | yes | See [Consistency model](consistency.md#durability-logging-and-checkpoints) |
| `flushing_batch_length` | int ≥ 1 | yes | Max records batched into one WAL flush |
| `flushing_batch_timeout` | duration > 0 | yes | Max time a batch waits before flushing short of `flushing_batch_length` |
| `queue_capacity` | int ≥ 0 | no | Pending-record queue size; if set, must be ≥ `flushing_batch_length` |
| `max_segment_size` | size | yes | WAL segment file rotates after this size |
| `data_directory` | path | yes | Where WAL segments and their `.meta` sidecars are written |

## `dump`

Required (and validated) when `persistence.mode` is `wal_and_dump` or `dump_only`.

```yaml
dump:
  interval: 1m
  directory: ./fq_data/
```

| Key | Type | Required | Notes |
|---|---|---|---|
| `interval` | duration > 0 | yes | How often a full dump snapshot is written |
| `directory` | path | yes | Where dump files are written |

## `engine`

```yaml
engine:
  type: in_memory
  partitions: 16
  wal_apply_workers: 1
  key_index: false
  clean_interval: 5m
  limit_event_queue_capacity: 16
```

| Key | Type | Required | Default if `0`/omitted | Notes |
|---|---|---|---|---|
| `type` | `in_memory` | yes | — | The only supported value |
| `clean_interval` | duration > 0 | yes | — | How often expired keys are swept |
| `partitions` | int ≥ 0 | no | 16 | Independent in-memory hash-table shards; see [Consistency model](consistency.md#concurrency-control) |
| `wal_apply_workers` | int ≥ 0 | no | 1 | Goroutines applying one WAL chunk on a replica |
| `limit_event_queue_capacity` | int ≥ 0 | no | 16 | Per-subscriber queue size for `STREAM` events |
| `key_index` | bool | no | `false` | Enables `SCAN`/`PSCAN`; adds write-path bookkeeping when on |

If `partitions` is omitted or set to `0`, fq uses the default value `16`.

## `replication`

```yaml
replication:
  replica_type: master # master | slave, omit to disable replication
  replica_id: "replica-1" # required when replica_type is slave
  master_address: ":1946"
  sync_interval: 1s
  auth:
    token_env: FQ_REPLICATION_TOKEN
  tls: {}
```

| Key | Type | Required | Notes |
|---|---|---|---|
| `replica_type` | `master` \| `slave` \| omitted | no | Omit to run standalone with no replication port |
| `replica_id` | string | required when `slave` | Identifies this replica in master-side tracking and metrics |
| `master_address` | `host:port` | no, defaults to `:1946` | Replication listen address (master) or dial address (slave) |
| `sync_interval` | duration ≥ 0 | no | How often a slave polls the master for new WAL data |
| `auth` | [`ReplicationAuthConfig`](#replicationauth) | required when `replica_type` is set | No unauthenticated mode — the server refuses to start without it |
| `tls` | [`TLSConfig`](#tls-network-and-replication) | no | Interpreted per `replica_type`, see below |

Setting `replica_type` also requires `persistence.mode: wal_and_dump`.

### `replication.auth`

```yaml
auth:
  token_env: FQ_REPLICATION_TOKEN
```

Exactly one of `token_env` or `token_file`. The master and every slave must resolve
this to the **same secret**; the master compares it in constant time on every request
and rejects mismatches with replication error code `3002`.

## `tls` (network and replication)

Both `network.tls` and `replication.tls` use the same shape, interpreted differently
by role:

```yaml
tls:
  cert_file: ./certs/server.crt
  key_file: ./certs/server.key
  client_ca_file: ./certs/ca.crt
  ca_file: ./certs/ca.crt
  server_name: fq.internal
  skip_verify: false
  min_version: "1.3"
```

| Key | Used by | Notes |
|---|---|---|
| `cert_file` / `key_file` | server; replica in mutual TLS | Must be set together or not at all |
| `client_ca_file` | server | Setting this turns on mutual TLS (client certificate required and verified) |
| `ca_file` | client (replica) | Trust anchor for the server certificate |
| `server_name` | client (replica) | Expected name in the server certificate |
| `skip_verify` | client (replica) | Disables server certificate verification — testing only |
| `min_version` | either | `1.2` (default) or `1.3` |

For `network.tls`, the server role always applies (`cert_file`/`key_file`, optional
`client_ca_file`). For `replication.tls`, the fields are read according to
`replica_type`: a master uses `cert_file`, `key_file`, `client_ca_file`, and
`min_version`; a slave uses `ca_file`, `server_name`, `skip_verify`, and — for mutual
TLS — its own `cert_file` and `key_file`. A master with `replication.tls` configured
must set both `cert_file` and `key_file`.

See [Operations](operations.md) for generating development certificates and running
with TLS end to end.

## `observability`

```yaml
observability:
  address: ":2112"
  pprof: false
```

| Key | Type | Required | Notes |
|---|---|---|---|
| `address` | `host:port` | no | Omit to disable the health/metrics HTTP server entirely |
| `pprof` | bool | no | Exposes `/debug/pprof/*` when true — see [Operations](operations.md) |

## `logging`

```yaml
logging:
  level: info # debug | info | warn | error
```

| Key | Type | Required | Notes |
|---|---|---|---|
| `level` | enum | yes | One of `debug`, `info`, `warn`, `error` |
