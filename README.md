# fq
Frequency-capping database

> **WARNING**
> Not for production use!

This is a small database for the frequency-capping functionality.
This is an experimental database, you should not use it in production.

## Why?

To learn databases implementation.

## Commands

The database supports the following commands:
 - **INCR** < key > < capping > - Increment counter for a key
 - **GET** < key > < capping > - Get current counter value for a key
 - **DEL** < key > < capping > - Delete a key
 - **MDEL** < key > < capping > < key > < capping > < key > < capping > ... - Delete multiple keys
 - **WATCH** < key > < capping > - Watch for changes to a key's value (blocks until value changes or timeout)

< key > - is some string key for which you want to be able to increment the counter for a time interval of size < capping >.

### WATCH Command

The **WATCH** command allows you to monitor a key for value changes. When executed, it:
- Blocks and waits for the key's value to change
- Polls the key every 100ms
- Returns the new value as soon as it changes
- Times out after 30 seconds if no changes are detected
- Can be cancelled with Ctrl+C

Example:
```
[fq]> WATCH mykey 600
Watching for changes... (press Ctrl+C to cancel)
[fq]> 5                    Elapsed: 1.234s
```

## Usage

### Building

Build binaries:
```shell
make build
```

This will create binaries in the `bin/` directory:
- `bin/fq` - database server
- `bin/fq-cli` - CLI client

### Running

#### Master Server

Run the master database server:
```shell
make run-server
```

Or directly:
```shell
go run ./cmd/fq
```

#### Slave Replica

Run a slave replica (in a separate terminal):
```shell
make run-slave
```

Or directly:
```shell
go run ./cmd/fq config-slave.yml
```

#### CLI Client

Connect to master (port :1945):
```shell
make run-cli
```

Or connect to slave (port :1947):
```shell
go run ./cmd/cli -address :1947
```

### Example

1. Start master server:
   ```shell
   make run-server
   ```

2. Start slave replica (in another terminal):
   ```shell
   make run-slave
   ```

3. Connect CLI client to master:
   ```shell
   make run-cli
   ```

4. Execute commands:
   ```
   [fq]> INCR key 600
   1
   [fq]> INCR key 600
   2
   [fq]> WATCH key 600
   Watching for changes... (press Ctrl+C to cancel)
   [fq]> 3                    Elapsed: 0.567s
   ```

5. Connect CLI client to slave (in another terminal):
   ```shell
   go run ./cmd/cli -address :1947
   ```

6. Read replicated data:
   ```
   [fq]> GET key 600
   2
   ```

Example of using commands in CLI tool:

<img src="docs/cli_commands.png" alt="Commands example" width="600"/>

Also you can use GoLang client: [fq-client-go](https://github.com/rom8726/fq-client-go)

## Architecture

### Storage Layer

- **WAL (Write-Ahead Log)**: All write operations are logged to disk before being applied to the engine
- **Periodic Dumps**: Data is periodically dumped to disk for recovery and replication
- **In-Memory Engine**: Fast in-memory hash table for data storage

### Replication

The database supports **master-slave replication**:

- **Master**: Accepts write operations and replicates them to slaves
- **Slave**: Connects to master, synchronizes initial dump, and continuously replicates WAL segments

#### Replication Features

- **Initial Dump Synchronization**: Slave first synchronizes the complete database dump from master
- **WAL Replication**: After dump synchronization, slave continuously replicates WAL segments
- **Real-time Updates**: Slave receives updates from master with configurable sync interval (default: 1s)
- **Automatic Reconnection**: Slave automatically reconnects to master on network errors
- **Exponential Backoff**: Retry mechanism with exponential backoff for error handling
- **Session Management**: Master manages dump read sessions with TTL and cleanup

#### Configuration

Master configuration (`config.yml`):
```yaml
replication:
  replica_type: master
  master_address: ":1946"  # Port for replication server
  sync_interval: 1s
```

Slave configuration (`config-slave.yml`):
```yaml
replication:
  replica_type: slave
  master_address: ":1946"  # Master replication address
  sync_interval: 1s
```

### Data Flow

1. **Write Operation**: Client sends write command to master
2. **WAL Write**: Master writes operation to WAL
3. **Engine Update**: Master applies operation to in-memory engine
4. **Replication**: Master sends WAL segment to slave (periodically)
5. **Slave Apply**: Slave receives WAL segment and applies to its engine

## Development

### Running Tests

```shell
make test
```

### Linting

```shell
make lint
```

### Building Protocol Buffers

```shell
make proto.wal.build
```
