# fq-bench

`fq-bench` runs live load benchmarks against a running fq server and writes text,
JSON, or CSV result reports.

Typical usage:

```shell
go run ./cmd/bench -profile benchmarks/profiles/smoke.yml
go run ./cmd/bench -address :1945 -connections 200 -duration 60s -query "RLIMIT FW {key} 10000 {batch}"
```

Profiles live in `benchmarks/profiles`. A profile can define the address, connection
count, warmup, duration, target RPS, request timeout, idle timeout, initial max
message size, query template, key distribution, key range, batch/window value, output
format, output path, and seed. CLI flags override profile values.

For publishable benchmark runs, use `cmd/results` instead of invoking `cmd/bench`
directly. See `docs/benchmarking.md` for the release methodology.
