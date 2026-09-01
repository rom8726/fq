# fq-report

`fq-report` converts benchmark JSON files produced by `fq-bench` into a Markdown
summary report.

Typical usage:

```shell
go run ./cmd/report -input_dir benchmarks/results -output_file benchmarks/results/report.md
```

The command reads benchmark JSON reports from `-input_dir`, sorts them, and renders
throughput, latency percentiles, error rate, profile metadata, and comparison notes.
Use `-output_file -` to print the Markdown report to stdout.

Published reports live in `benchmarks/reports`. See `docs/benchmarking.md` for what
metadata and run conditions should be included in a public report.
