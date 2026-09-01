# fq-results

`fq-results` plans and optionally executes reproducible benchmark and stress result
runs.

Typical usage:

```shell
go run ./cmd/results -mode smoke
go run ./cmd/results -mode smoke -run -benchmarks=false
go run ./cmd/results -mode release \
  -server_info_url http://db-host:2112/v1/info \
  -token_env FQ_RW_TOKEN \
  -tls_ca ./certs/ca.crt \
  -tls_server_name fq.internal \
  -run -confirm_release_run
```

The command creates a timestamped run directory under `benchmarks/results/runs` with
metadata, config/profile snapshots, a command manifest, command logs, JSON benchmark
reports, JSON stress reports, and a Markdown summary.
When `-server_info_url` is set, the command also stores the database server's
`/v1/info` response as `server-info.json` for publication reports.
Benchmark commands use the `address` from each profile by default. When benchmark
profiles are enabled and `-address` is not passed, every planned profile must define
`address`; otherwise `cmd/results` stops before the run instead of silently falling
back to `:1945`. Pass `-address host:port` to `cmd/results` only when you want to
override all profiles for one run.
Benchmark authentication is passed to `cmd/bench` through its private `FQ_TOKEN`
environment. Use one of `-token`, `-token_env`, or `-token_file`; the token value is
not written to the command manifest.

Release mode is intentionally guarded by `-confirm_release_run` when `-run` is set,
because it executes the full publishable workload. See `docs/benchmarking.md` for the
rules for public benchmark reports.
