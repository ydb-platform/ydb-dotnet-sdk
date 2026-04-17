# Ydb.Sdk.OpenTelemetry

[![NuGet](https://img.shields.io/nuget/v/Ydb.Sdk.OpenTelemetry)](https://www.nuget.org/packages/Ydb.Sdk.OpenTelemetry)

OpenTelemetry extension methods for [Ydb.Sdk](https://www.nuget.org/packages/Ydb.Sdk) — registers the `Ydb.Sdk` activity source and meter into your OpenTelemetry pipeline.

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

| Span | Kind | Description |
|---|---|---|
| `ydb.Driver.Initialize` | Internal | Driver first initialization (discovery + auth handshake) |
| `ydb.CreateSession` | Client | Session creation (gRPC CreateSession + AttachStream) |
| `ydb.RunWithRetry` | Internal | Wraps the entire retry loop for a single ADO.NET operation |
| `ydb.Try` | Internal | One span per attempt, including the first; child RPC spans attach here |
| `ydb.ExecuteQuery` | Client | Individual YQL query execution |
| `ydb.Commit` | Client | Transaction commit |
| `ydb.Rollback` | Client | Transaction rollback |

Tags propagated on RPC spans:

| Tag | Description |
|---|---|
| `db.system.name` | Always `"ydb"` |
| `db.namespace` | YDB database path |
| `server.address` / `server.port` | Primary endpoint from connection string |
| `network.peer.address` / `network.peer.port` | Actual gRPC endpoint used for the call |
| `ydb.node.id` / `ydb.node.dc` | YDB node identity |
| `db.response.status_code` | YDB status code (on `YdbException`) |
| `error.type` | `"transport_error"` / `"ydb_error"` / full exception type name |

W3C trace context (`traceparent`) is automatically propagated to the YDB server so server-side traces link to client spans.

## Metrics (beta)

Meter name: `Ydb.Sdk`.

| Metric | Kind | Unit | Description |
|---|---|---|---|
| `db.client.operation.duration` | Histogram | `s` | Latency of ADO.NET operations (`ExecuteQuery`, `Commit`, `Rollback`) |
| `ydb.client.operation.failed` | Counter | `{command}` | Count of failed operations |
| `ydb.query.session.count` | ObservableUpDownCounter | `{connection}` | Current session pool counts (`idle` / `used`) |
| `ydb.query.session.create_time` | Histogram | `s` | Time to create a new session |
| `ydb.query.session.pending_requests` | UpDownCounter | `{request}` | Requests waiting for a free session |
| `ydb.query.session.timeouts` | Counter | `{connection}` | Session acquisition timeouts |

Pool-scoped metrics carry the `ydb.query.session.pool.name` tag — set via the `PoolName` connection string option, defaults to the full connection string.

## Example

See the [OpenTelemetry E2E playground](../../examples/Ydb.Sdk.AdoNet.OpenTelemetry) for a full Docker Compose stack with Grafana, Tempo, and Prometheus.

## Changelog

See [CHANGELOG.md](./CHANGELOG.md).
