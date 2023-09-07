# SLO playground

Playground may be used for testing SLO workloads locally

It has several services:

- `prometheus` - storage for metrics
- `prometheus-pushgateway` - push acceptor for prometheus
- `grafana` - provides chats for metrics
- `ydb` - local instance of ydb-database to run workload with

## Network addresses

- Grafana dashboard: http://localhost:3000
- Prometheus pushgateway: http://localhost:9091
- YDB monitoring: http://localhost:8765
- YDB GRPC: grpc://localhost:2136
- YDB GRPC TLS: grpcs://localhost:2135

## Start

```shell
docker-compose up -d
```

## Stop

```shell
docker-compose down
```

## Configs

Grafana's dashboards stored in `configs/grafana/provisioning/dashboards`

## Data

YDB databases are not persistent

All other data like metrics and certs stored in `data/`