# fq-results

`fq-results` plans and optionally executes reproducible benchmark and stress result
runs.

Typical usage:

```shell
go run ./cmd/results -mode smoke
go run ./cmd/results -mode smoke -run -benchmarks=false
go run ./cmd/results -mode release -run -confirm_release_run
```

The command creates a timestamped run directory under `benchmarks/results/runs` with
metadata, config/profile snapshots, a command manifest, command logs, JSON benchmark
reports, JSON stress reports, and a Markdown summary.

Release mode is intentionally guarded by `-confirm_release_run` when `-run` is set,
because it executes the full publishable workload. See `docs/benchmarking.md` for the
rules for public benchmark reports.
