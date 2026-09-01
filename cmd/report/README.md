# fq-report

`fq-report` converts benchmark JSON files produced by `fq-bench` into a Markdown
summary report.

Typical usage:

```shell
go run ./cmd/report -input benchmarks/results/runs/20260828T060000Z-release-host-abc123-release
go run ./cmd/report -input benchmarks/results -output -
```

The command reads either a flat directory of benchmark JSON reports or a full
`cmd/results` run directory containing `benchmarks/`, `stress/`, `metadata.json`, and
`manifest.json`. If `server-info.json` is present, the report renders database-server
metadata separately from benchmark-client metadata. It renders throughput, latency
percentiles, error rate, profile metadata, run metadata, stress results, and
comparison notes. Use `-output -` to print the Markdown report to stdout.

By default the report is written to `benchmarks/reports/report_YYYY_MM_DD.md`, using
the current local date. `-input_dir` and `-output_file` are accepted as aliases for
`-input` and `-output`.

Published reports live in `benchmarks/reports`. See `docs/benchmarking.md` for what
metadata and run conditions should be included in a public report.
