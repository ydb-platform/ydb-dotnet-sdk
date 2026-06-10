# YDB .NET SDK — Examples

Ready-to-run sample projects for working with YDB in .NET.
Each project targets a specific use case and can be started with `dotnet run`.

## ADO.NET

| Project | Description |
|---|---|
| [Ydb.Sdk.AdoNet.QuickStart](./Ydb.Sdk.AdoNet.QuickStart) | Minimal ADO.NET: connect, create table, insert, query |
| [Ydb.Sdk.AdoNet.Dapper.QuickStart](./Ydb.Sdk.AdoNet.Dapper.QuickStart) | Same workflow using the Dapper micro-ORM |
| [Ydb.Sdk.AdoNet.Yandex.Cloud](./Ydb.Sdk.AdoNet.Yandex.Cloud) | Connect to a managed YDB instance in Yandex Cloud using a service account key |
| [Ydb.Sdk.AdoNet.Yandex.Cloud.Serverless.Container](./Ydb.Sdk.AdoNet.Yandex.Cloud.Serverless.Container) | Deploy to Yandex Cloud Serverless Containers (stateless `YdbDataSource` per request) |

## OpenTelemetry

| Project | Description |
|---|---|
| [Ydb.Sdk.AdoNet.OpenTelemetry](./Ydb.Sdk.AdoNet.OpenTelemetry) | Full observability stack: traces → Tempo, metrics → Prometheus, dashboards in Grafana. Includes a load-tank and a tracing demo app. |

Start the whole stack with:

```bash
cd Ydb.Sdk.AdoNet.OpenTelemetry
docker compose -f compose-e2e.yaml up -d
```

Open Grafana at `http://localhost:3000`.

## Topic (Pub/Sub)

| Project | Description |
|---|---|
| [Ydb.Sdk.Topic.QuickStart](./Ydb.Sdk.Topic.QuickStart) | Write and read messages using `TopicWriter<T>` / `TopicReader<T>` |

## Entity Framework Core

| Project | Description |
|---|---|
| [EntityFrameworkCore.Ydb.QuickStart](./EntityFrameworkCore.Ydb.QuickStart) | EF Core basics: define model, run migrations, query with LINQ |
| [EntityFrameworkCore.Ydb.Samples](./EntityFrameworkCore.Ydb.Samples) | Extended samples: LINQ operations, one-to-one / one-to-many / many-to-many schemas |
| [EntityFrameworkCore.Ydb.Yandex.Cloud](./EntityFrameworkCore.Ydb.Yandex.Cloud) | EF Core with Yandex Cloud authentication |

## Linq2db

| Project | Description |
|---|---|
| [Linq2db.QuickStart](./Linq2db.QuickStart) | Using Linq2DB query builder with YDB via the ADO.NET provider |

## Prerequisites

All examples require a running YDB instance. The quickest way to start one locally:

```bash
docker run -d --name ydb-local -p 2136:2136 -p 8765:8765 \
  -e YDB_USE_IN_MEMORY_PDISKS=true \
  ydbplatform/local-ydb:latest
```

YDB UI is then available at `http://localhost:8765`.
