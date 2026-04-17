# SLO Testing for YDB .NET SDK

**SLO (Service Level Objectives)** tests verify SDK reliability under adverse conditions:
node failures, tablet restarts, and network partitions — the kind of events that happen routinely
in a large distributed database cluster.

## Structure

```
slo/
├── playground/   Local Docker Compose stack (YDB + Prometheus + Grafana)
└── src/          SLO workload tools (AdoNet, TopicService, Linq2db)
```

## Quick Start (local)

### 1. Start the playground

```bash
cd slo/playground
docker compose up -d
```

Services:

| Service | URL |
|---|---|
| Grafana | http://localhost:3000 |
| Prometheus Pushgateway | http://localhost:9091 |
| YDB monitoring | http://localhost:8765 |
| YDB gRPC | grpc://localhost:2136 |

### 2. Run the workload

```bash
cd slo/src/AdoNet

# Create the test table
dotnet run -- create grpc://localhost:2136 /local

# Run read/write workload for 10 minutes
dotnet run -- run grpc://localhost:2136 /local \
  --prom-pgw http://localhost:9091 \
  --read-rps 1000 --write-rps 100 --time 600

# Drop the table when done
dotnet run -- cleanup grpc://localhost:2136 /local
```

### 3. View metrics in Grafana

Open `http://localhost:3000` and import the SLO dashboard from the
[slo-tests repository](https://github.com/ydb-platform/slo-tests/blob/main/k8s/helms/grafana.yaml#L69).

## Detailed Documentation

- [Workload CLI reference](./src/README.md) — all commands and arguments
- [Playground setup](./playground/README.md) — Docker Compose services and config
