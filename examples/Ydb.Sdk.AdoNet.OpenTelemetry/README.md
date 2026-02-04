# Ydb.Sdk.AdoNet.OpenTelemetry (E2E playground)

Goal: spin up a local OpenTelemetry stack and verify that a basic .NET app exports traces/metrics via `otel-collector` into `Tempo`/`Prometheus`, with visualization in `Grafana`.

## How to run

From `examples/Ydb.Sdk.AdoNet.OpenTelemetry`:

```bash
# Preferred (Docker Compose v1 / legacy binary)
docker-compose -f compose-e2e.yaml up -d

# Alternative (Docker Compose v2 plugin, if available)
# docker compose -f compose-e2e.yaml up -d
```

Note: the `ydbplatform/local-ydb` image is commonly used as `linux/amd64`. On Apple Silicon/arm64 (e.g. Colima), you may need x86_64 emulation (Rosetta) depending on your Docker VM setup.

## Enable server-side tracing in YDB (end-to-end)

This demo already exports app traces to the collector. To also export **YDB server traces** into the same collector:

```bash
# 1) edit ./ydb_config/ydb-config.yaml and add tracing_config
#    (see ./ydb_config/otel-tracing-snippet.yaml)
#
# 2) recreate the ydb container to pick up the updated config
docker-compose -f compose-e2e.yaml up -d --force-recreate ydb
```

## What should be running

- **YDB local UI**: `http://localhost:8765`
- **Grafana**: `http://localhost:3000` (anonymous access enabled)
- **Tempo API**: `http://localhost:3200`
- **Prometheus**: `http://localhost:9090`
- **OTel Collector**
  - OTLP gRPC: `localhost:4317`
  - OTLP HTTP: `http://localhost:4318`
  - healthcheck: `http://localhost:13133`
  - zPages: `http://localhost:55679/debug/tracez`

## YDB metrics → OTel Collector → Prometheus

Prometheus scrapes **the collector's Prometheus exporter** at `otel-collector:9464`.

- If your .NET app exports metrics via OTLP, you will see them in Prometheus/Grafana.
- YDB metrics are **not** scraped in this demo by default (YDB → Collector metrics wiring depends on how YDB exposes metrics in your setup/version).

## How to verify traces

### Run the example app locally (recommended for debugging)

From `examples/Ydb.Sdk.AdoNet.OpenTelemetry`:

```bash
dotnet run -c Release --project "Ydb.Sdk.AdoNet.OpenTelemetry.csproj"
```

1) Open Grafana → **Explore** → select datasource **Tempo**

2) Find service **`ydb-sdk-adonet-sample`** (default) and inspect spans `app.startup` / `app.tick`.

Tip: spans are also printed in `otel-collector` logs (the `debug` exporter is enabled).
