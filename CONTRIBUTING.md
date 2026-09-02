# Contributing

Thanks for helping improve fq.

## Development Setup

fq is a Go project. Use the Go version declared in `go.mod`; Docker build images are
pinned to the same Go line.

```shell
go version
go test ./...
```

Useful Make targets:

```shell
make build
make test
make test-race
make lint
make stress-crash-loop
make stress-dump-recovery
make stress-replication
```

The lint target expects `golangci-lint` to be installed locally. Docker is optional
unless you are testing images, packages, or protobuf generation.

## Before Opening a Pull Request

- Keep changes focused on one behavior or maintenance task.
- Add or update tests for behavioral changes.
- Update docs when commands, configuration, protocol behavior, deployment steps, or
  operational guidance changes.
- Run `go test ./...` before submitting. Run `make test-race` for concurrency,
  network, storage, or replication changes.
- Add a `CHANGELOG.md` entry for user-visible changes.

## Commit and PR Style

Use clear, imperative commit messages when practical. In pull requests, explain:

- what changed;
- why it changed;
- how it was tested;
- any compatibility, migration, or operational risk.

## Security

Do not include secrets, tokens, private hostnames, or customer data in issues, pull
requests, logs, benchmarks, or reports. Follow `SECURITY.md` for vulnerability reports.
