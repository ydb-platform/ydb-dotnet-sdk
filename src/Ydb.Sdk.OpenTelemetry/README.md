# Ydb.Sdk.OpenTelemetry

[![NuGet](https://img.shields.io/nuget/v/Ydb.Sdk.OpenTelemetry)](https://www.nuget.org/packages/Ydb.Sdk.OpenTelemetry)

OpenTelemetry extension methods for [Ydb.Sdk](https://www.nuget.org/packages/Ydb.Sdk) — registers the `Ydb.Sdk` activity
source and meter into your OpenTelemetry pipeline.

## Installation

```bash
dotnet add package Ydb.Sdk.OpenTelemetry
```

## Usage

```csharp
services.AddOpenTelemetry()
    .WithTracing(b => b.AddYdb())
    .WithMetrics(b => b.AddYdb());
```

## Tracing

ActivitySource name: `Ydb.Sdk`.

| Span                    | Kind     | Description                                                            |
|-------------------------|----------|------------------------------------------------------------------------|
| `ydb.Driver.Initialize` | Internal | Driver first initialization (discovery + auth handshake)               |
| `ydb.CreateSession`     | Client   | Session creation (gRPC CreateSession + AttachStream)                   |
| `ydb.RunWithRetry`      | Internal | Wraps the entire retry loop for a single ADO.NET operation             |
| `ydb.Try`               | Internal | One span per attempt, including the first; child RPC spans attach here |
| `ydb.ExecuteQuery`      | Client   | Individual YQL query execution                                         |
| `ydb.Commit`            | Client   | Transaction commit                                                     |
| `ydb.Rollback`          | Client   | Transaction rollback                                                   |

Tags propagated on RPC spans:

| Tag                                          | Description                                                    |
|----------------------------------------------|----------------------------------------------------------------|
| `db.system.name`                             | Always `"ydb"`                                                 |
| `db.namespace`                               | YDB database path                                              |
| `server.address` / `server.port`             | Primary endpoint from connection string                        |
| `network.peer.address` / `network.peer.port` | Actual gRPC endpoint used for the call                         |
| `ydb.node.id` / `ydb.node.dc`                | YDB node identity                                              |
| `db.response.status_code`                    | YDB status code (on `YdbException`)                            |
| `error.type`                                 | `"transport_error"` / `"ydb_error"` / full exception type name |

W3C trace context (`traceparent`) is automatically propagated to the YDB server so server-side traces link to client
spans.

## Metrics (beta)

Meter name: `Ydb.Sdk`.

Every `ydb.query.session.*` metric carries **`ydb.query.session.pool.name`** (via `PoolName` in the connection string; otherwise the full connection string).

| Metric                               | Kind              | Unit          | Attributes | Description |
|--------------------------------------|-------------------|---------------|------------|-------------|
| `ydb.client.operation.duration`      | Histogram         | `s`           | `database`, `endpoint`, `operation.name` | Latency of each actual `ExecuteQuery`, `Commit`, or `Rollback` attempt. |
| `ydb.client.operation.failed`        | Counter           | `{operation}` | `database`, `endpoint`, `operation.name`, `status_code` | Unsuccessful operation attempts. |
| `ydb.query.session.create_time`      | Histogram         | `s`           | `ydb.query.session.pool.name` | Cost of session creation (CreateSession + first AttachStream message). |
| `ydb.query.session.pending_requests` | Counter           | `{request}`   | `ydb.query.session.pool.name` | Increments when a caller starts waiting for a session; use **rate** (not level) for queue pressure. |
| `ydb.query.session.timeouts`         | Counter           | `{timeout}`   | `ydb.query.session.pool.name` | Pool could not satisfy demand within the acquisition timeout. |
| `ydb.query.session.count`            | ObservableGauge   | `{session}`   | `ydb.query.session.pool.name`, `ydb.query.session.state` (`idle` / `used`) | Current pool occupancy. |
| `ydb.query.session.max`              | ObservableGauge   | `{session}`   | `ydb.query.session.pool.name` | Configured `MaxPoolSize` (context). |
| `ydb.query.session.min`              | ObservableGauge   | `{session}`   | `ydb.query.session.pool.name` | Configured `MinPoolSize` (context). |

`database` is the YDB database path; `endpoint` is `host:port` from the connection string.

## Example

See the [OpenTelemetry E2E playground](../../examples/Ydb.Sdk.AdoNet.OpenTelemetry) for a full Docker Compose stack with
Grafana, Tempo, and Prometheus.

## Changelog

See [CHANGELOG.md](./CHANGELOG.md).
