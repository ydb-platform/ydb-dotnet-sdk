# SLO Workload CLI

A load generator that drives read/write traffic against a YDB cluster and exports metrics
over OTLP. The same harness is shared between the table workloads (`AdoNet`, `Dapper`, `EF`,
`Linq2db`) and the topic workload (`TopicService`).

The CLI is invoked by [`ydb-platform/ydb-slo-action`](https://github.com/ydb-platform/ydb-slo-action);
you normally don't run it by hand.

## `run` — execute the workload

```bash
slo run <connectionString> [options]
```

| Argument / Option | Default | Description |
|---|---|---|
| `<connectionString>` | — | YDB connection string in ADO.NET format, e.g. `Host=ydb;Port=2136;Database=/Root/testdb` |
| `--otlp-endpoint` | — | OTLP metrics endpoint. Falls back to `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`, then `OTEL_EXPORTER_OTLP_ENDPOINT` (with `/v1/metrics` appended) |
| `--report-period` | `1000` | Metrics export period (ms) |
| `--read-rps` | `1000` | Target read RPS |
| `--read-timeout` | `1000` | Read timeout (ms) |
| `--write-rps` | `1000` | Target write RPS |
| `--write-timeout` | `100` | Write timeout (ms) |
| `--time` | `600` | Run duration (s). Falls back to `WORKLOAD_DURATION` env var |
| `-c, --initial-data-count` | `1000` | Rows to seed before the run starts |

The harness creates the test table (or topic) and seeds initial data if it does not yet exist —
there are no separate `create`/`cleanup` commands.

## Metrics

Latency and error counts are exported as OpenTelemetry metrics with `operation_type`
(`read` / `write`) and `ref` (the workload Git ref) attributes. The action's Prometheus
instance scrapes them and the report step compares the current run against `main`.

## Auth

Anonymous credentials. The action wires up a local cluster that does not require auth.
