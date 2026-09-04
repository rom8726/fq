# fq wire protocol, version 1

This document is the normative description of the protocol spoken on the fq client
port. Where the README and this document disagree, this document wins. For usage
examples and prose per command, see [Commands](commands.md).

## Version and compatibility

The current protocol version is **1**. A connection must negotiate it with `HELLO`
before issuing any other command.

Changes that do **not** require a new protocol version:

- rewording the human-readable message of an error response;
- adding a new code inside an existing range;
- adding a new command;
- appending a field to the end of an existing `ok|` response.

Changes that **do** require a new protocol version:

- changing the shape of an existing command's response;
- removing a command;
- changing the meaning of an existing code.

Clients must therefore match on the numeric code, never on the message text.

## Framing

Every message — request or response — is a frame: a four-byte big-endian unsigned
length followed by exactly that many bytes of payload.

```text
+--------+--------+--------+--------+---------------------------+
|            length (uint32)        |          payload          |
+--------+--------+--------+--------+---------------------------+
```

The payload size limit is the `max_message_size` reported by the handshake. fq keeps
the default small (`4KB`) so ordinary client traffic uses bounded, predictable
per-connection buffers; the setting can be raised, but it applies to the whole client
port, not to one command. A request larger than the limit is refused with code 1004; a
non-chunked server response that would exceed it closes the connection.

## Grammar

```abnf
frame        = length payload
length       = 4OCTET                  ; big-endian uint32 = len(payload)
payload      = request / response

request      = command *( SP argument )
response     = ok-response / err-response / nxt-response

ok-response  = "ok|" [ result ]
err-response = "err|" code "|" message
nxt-response = "nxt|" chunk

code         = 4DIGIT
message      = *OCTET
result       = field *( ";" field )
```

`code` is stable and part of the contract. `message` is for humans and may change in
any release.

## Handshake

```text
HELLO <version> [AUTH <token>]
```

The response is:

```text
ok|<version>;<max_message_size>;<auth_required>;<role>
```

- `version` — the negotiated protocol version;
- `max_message_size` — maximum payload size in bytes, for sizing client buffers;
- `auth_required` — `1` when the server has authentication configured, otherwise `0`;
- `role` — the connection's current role: `none`, `ro`, `rw` or `admin`. A server
  without authentication reports `admin`.

Example exchange:

```text
-> HELLO 1 AUTH s3cret-admin-token-value
<- ok|1;65536;1;admin
-> GET user_42 60
<- ok|7
```

Rules of ordering:

1. An unsupported version answers `err|1011|unsupported protocol version: <n>`. The
   connection stays open and unnegotiated; the client may retry `HELLO` with another
   version.
2. A supported version is negotiated immediately — before the inline `AUTH` is
   attempted.
3. A failed inline `AUTH` answers `err|3002|authentication failed`, but the version
   remains negotiated. The client retries `AUTH` alone; it does not repeat `HELLO`.

Any command sent before a successful `HELLO`, **including `AUTH`**, answers
`err|1010|handshake required`. Repeating `HELLO` with the same version is idempotent
and is the way to re-read the role after a separate `AUTH`. Repeating it with a
different version answers `err|1012|protocol version already negotiated`.

The token is an opaque literal, so base64 values containing `=` or `+` work as-is, and
it is never written to the logs.

## Responses

Every response begins with one of three tags.

| Tag | Meaning |
|---|---|
| `ok\|` | Success. The remainder is the result, whose fields are separated by `;`. |
| `err\|` | Failure. The remainder is a four-digit code, `\|`, and a human-readable message. |
| `nxt\|` | A non-final chunk of a response that does not fit one frame. |

A chunked response is a sequence of `nxt|` frames terminated by exactly one `ok|` or
`err|` frame. The frame limit still applies to every chunk: with a `4KB` limit, a
non-final chunk can carry at most `4096 - len("nxt|")` bytes of response body. Clients
concatenate the payloads to reconstruct the whole document:

```text
nxt|<partial JSON>
nxt|<partial JSON>
ok|<final partial JSON>
```

A response that fits one frame is returned directly, so a client that never encounters
`nxt|` needs no special handling. Today `INSPECT` is the only command that chunks; its
reports are expected to exceed the default frame size on sufficiently busy instances.

## Error codes

The range identifies the category, so a client can react to `code / 1000` without
knowing every code. Unused numbers inside a range are reserved for future errors of the
same category. Codes are never reused for a different meaning.

### 1xxx — protocol and request parsing

| Code | Message | When |
|---|---|---|
| 1000 | invalid symbol | The request contains a byte the tokenizer does not accept |
| 1001 | invalid command | The first token is not a known command |
| 1002 | invalid arguments | Arguments are malformed or of the wrong shape |
| 1003 | invalid arguments count | The command received the wrong number of arguments |
| 1004 | message size exceeds maximum | The request payload is larger than `max_message_size` |
| 1010 | handshake required | A command arrived before a successful `HELLO` |
| 1011 | unsupported protocol version | The requested version is not supported by this server |
| 1012 | protocol version already negotiated | `HELLO` asked for a different version on a negotiated connection |

### 2xxx — argument validation

| Code | Message | When |
|---|---|---|
| 2000 | key cannot be empty | The key argument is an empty string |
| 2001 | key length exceeds maximum | The key is longer than 1024 bytes |
| 2002 | batch is not a number | The window argument is not an unsigned integer |
| 2003 | invalid batch size | The window is outside the accepted range |
| 2004 | limit is not a number | The limit argument is not an unsigned integer |
| 2005 | invalid limit | The limit is outside the accepted range |
| 2006 | invalid rate limit algorithm | `RLIMIT` was given an algorithm other than `FW`, `SW` or `TB` |
| 2007 | invalid scan count | The `SCAN`/`PSCAN` count is outside the accepted range |
| 2008 | invalid scan cursor | The cursor was not produced by a previous scan |

### 3xxx — authentication and authorization

| Code | Message | When |
|---|---|---|
| 3000 | not authenticated | The server requires authentication and this connection has none |
| 3001 | permission denied | The connection's role does not cover this command |
| 3002 | authentication failed | The token was rejected |
| 3003 | too many authentication failures | Five failed attempts; the connection is closed |

### 4xxx — quotas

| Code | Message | When |
|---|---|---|
| 4000 | quota not found | No quota exists under that name |
| 4001 | quota limit mismatch | The declared limit differs from the stored one |
| 4002 | quota already acquired with different amount | The client holds an allocation of another size |
| 4003 | quota is not empty | `QUOTA DEL` on a quota that still has allocations |
| 4004 | quota limit is below used amount | The new limit is lower than what is already used |
| 4005 | quota ownership mismatch | Server-owned and client-leased operations were mixed |
| 4006 | quota policy mismatch | Fixed and per-client operations were mixed |

### 5xxx — instance state

| Code | Message | When |
|---|---|---|
| 5000 | scan index is disabled | `SCAN`/`PSCAN` needs the key index, which is off |
| 5001 | inspect is not available | The instance has no inspector configured |
| 5002 | inspect report too large | The report exceeds the 1 MiB cap |
| 5003 | max message size too small for a chunked response | The configured frame size cannot hold a chunk |
| 5004 | replica does not support the configured compression codec | The master stores compressed WAL segments and the replica did not advertise the codec |
| 5005 | instance is a read-only replica | A write command reached a replica; send writes to the master |

### 9xxx — internal

| Code | Message | When |
|---|---|---|
| 9000 | internal error | Fallback for any error without a code; the real text goes to the log only |
| 9001 | internal configuration error | The command layer and the handler disagree about a command |

Code 9000 deliberately hides the original message. Internal errors may carry file paths
and storage details, which do not belong on the wire.

## Command reference

Rate limiting:

```text
RLIMIT FW <key> <limit> <window>
RLIMIT SW <key> <limit> <window>
RLIMIT TB <key> <capacity> <refill_amount> <refill_window>
```

All three answer `ok|<allowed>;<current>;<remaining>;<reset_after>`.

Counters:

```text
INCR <key> <window>          -> ok|<value>
GET <key> <window>           -> ok|<value>
DEL <key> <window>           -> ok|<0|1>
MDEL <key> <window> ...      -> ok|<0|1>[;<0|1>...]
WATCH <key> <window>         -> ok|<value>
SCAN <cursor> <count>        -> ok|<next_cursor>[;<key>;<window>...]
PSCAN <prefix> <cursor> <count> -> ok|<next_cursor>[;<key>;<window>...]
```

Quotas:

```text
QUOTA SET <name> <limit>                        -> ok|<0|1>
QUOTA SETN <name> <limit> <clients>             -> ok|<0|1>
QUOTA ACQ <name> <amount> <client_id> [ttl]     -> ok|<acquired>;<allocated>;<used>;<remaining>;<expires_after>
QUOTA ACQN <name> <client_id> [ttl]             -> ok|<acquired>;<allocated>;<used>;<remaining>;<expires_after>
QUOTA ACQL <name> <limit> <amount> <client_id> [ttl] -> ok|<acquired>;<allocated>;<used>;<remaining>;<expires_after>
QUOTA REL <name> <client_id>                    -> ok|<0|1>
QUOTA DEL <name>                                -> ok|<0|1>
QUOTA INF <name>                                -> ok|<limit>;<used>;<remaining>[;<client_id>;<amount>;<expires_at>...]
```

Streams — each answers with a frame per event until the client disconnects:

```text
STREAM              -> ok|<key>;<window>;<current>;<reset_after>
PSTREAM <prefix>    -> ok|<key>;<window>;<current>;<reset_after>
QSTREAM             -> ok|<event>;<name>;<client_id>;<amount>;<used>;<remaining>;<expires_at>
QPSTREAM <prefix>   -> ok|<event>;<name>;<client_id>;<amount>;<used>;<remaining>;<expires_at>
```

Session and administration:

```text
HELLO <version> [AUTH <token>]  -> ok|<version>;<max_message_size>;<auth_required>;<role>
AUTH <token>                    -> ok|1
FLUSHDB                         -> ok|1
TRUNCATE                        -> ok|1
INSPECT [section]               -> chunked JSON, see above
```

Roles are hierarchical — `admin` includes `rw`, and `rw` includes `ro`:

| Role | Commands |
|---|---|
| `ro` | `GET`, `SCAN`, `PSCAN`, `WATCH`, `STREAM`, `PSTREAM`, `QSTREAM`, `QPSTREAM`, `QUOTA INF` |
| `rw` | everything in `ro`, plus `INCR`, `DEL`, `MDEL`, `RLIMIT`, and the remaining `QUOTA` subcommands |
| `admin` | everything in `rw`, plus `FLUSHDB`, `TRUNCATE`, and `INSPECT` |

`HELLO` and `AUTH` sit outside the role matrix.

## Replication protocol

Replication runs on its own port and is a **separate protocol with its own version
line**. Its version is currently 1 and moves independently of the client protocol
version above.

Messages are gob-encoded structures inside the same length-prefixed frames. They are
not text frames and carry no `ok|`/`err|`/`nxt|` tag, so a client library must read them
raw.

```go
type Request struct {
    AuthToken       string
    ProtocolVersion uint32
    DumpRequest
    WALRequest
}
```

A peer that does not declare a version sends `ProtocolVersion` as `0`, which the master
rejects. The master checks the shared secret first and the version second, so an
unauthenticated peer learns nothing — not even which versions exist.

`DumpResponse` and `WALResponse` carry `Succeed bool` and `ErrorCode`, drawn from the
same code registry as the client protocol: `3002` when the shared secret is rejected,
`1011` when the master refuses the replica's protocol version, and `9000` for an
internal failure while producing data. A rejected replica keeps reconnecting with
backoff, logs the code, and reports it as `repl.slave.last_error_code` in `INSPECT`.
