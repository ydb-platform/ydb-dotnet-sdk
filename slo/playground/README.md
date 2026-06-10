# SLO Playground

Local Docker Compose stack for running SLO workloads against a real YDB instance with
full metrics visibility in Grafana.

## Services

| Service | URL | Description |
|---|---|---|
| Grafana | http://localhost:3000 | Metrics dashboards |
| Prometheus Pushgateway | http://localhost:9091 | Push endpoint for workload metrics |
| Prometheus | http://localhost:9090 | Metrics storage |
| YDB monitoring | http://localhost:8765 | YDB cluster UI |
| YDB gRPC | grpc://localhost:2136 | YDB endpoint (plain) |
| YDB gRPC TLS | grpcs://localhost:2135 | YDB endpoint (TLS) |

## Usage

```bash
# Start all services
docker compose up -d

# Stop all services
docker compose down
```

## Configuration

- Grafana dashboards: `configs/grafana/provisioning/dashboards/`
- YDB is started in **non-persistent** mode — data is lost on container restart.
- Prometheus and Grafana data is stored in `data/` (persisted between restarts).

## Next Steps

After the playground is running, use the [SLO workload tool](../src/README.md) to create a
test table and drive load against YDB.
