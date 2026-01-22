# Ydb.Sdk.AdoNet.OpenTelemetry (E2E playground)

Goal: spin up a local OpenTelemetry stack and verify that a basic .NET app exports traces/metrics via `otel-collector` into `Tempo`/`Prometheus`, with visualization in `Grafana`.

## How to run

From `examples/Ydb.Sdk.AdoNet.OpenTelemetry`:

```bash
# ensure host directories for YDB volumes exist (helps with colima/docker permission quirks)
mkdir -p ydb_data ydb_certs

# Docker Compose v2 (plugin)
docker compose -f compose-e2e.yaml up --build

# Docker Compose v1 (legacy)
docker-compose -f compose-e2e.yaml up --build
```

Note: the `ydbplatform/local-ydb` image is commonly used as `linux/amd64`. On Apple Silicon/arm64 (e.g. Colima), you may need x86_64 emulation (Rosetta) depending on your Docker VM setup.

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

## How to verify traces

1) Open Grafana → **Explore** → select datasource **Tempo**

2) Find service **`ydb-sdk-adonet-sample`** (default) and inspect spans `app.startup` / `app.tick`.

Tip: spans are also printed in `otel-collector` logs (the `debug` exporter is enabled).
