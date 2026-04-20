# Ydb.Sdk

[![NuGet](https://img.shields.io/nuget/v/Ydb.Sdk)](https://www.nuget.org/packages/Ydb.Sdk)

Core .NET SDK for [YDB](https://ydb.tech) — a distributed SQL database by Yandex.
Includes a full ADO.NET provider and a Topic (Pub/Sub) client.

## Installation

```bash
dotnet add package Ydb.Sdk
```

## Features

- **Full ADO.NET support** — `DbConnection`, `DbCommand`, `DbDataReader`, `DbTransaction`, `DbDataSource`, `DbParameter`, and more
- **Session pooling** — configurable min/max pool size, idle timeout, session reuse
- **Retry policy** — exponential backoff with jitter; pluggable via `IRetryPolicy`
- **Transactions** — interactive (serializable) and snapshot read-only modes
- **Bulk upsert** — efficient batch insert via `YdbBulkUpsertCommand`
- **Topic client** — `TopicWriter<T>` / `TopicReader<T>` for Pub/Sub messaging
- **OpenTelemetry** — tracing and metrics via `Ydb.Sdk.OpenTelemetry` (opt-in)
- **Authentication** — anonymous, static token, service account (Yandex Cloud), metadata service
- **TLS support** — connect to YDB with `UseTls=true`

## Quick Start

The recommended entry point is `YdbDataSource` — it owns the session pool and connection lifecycle:

```csharp
await using var dataSource = new YdbDataSource("Host=localhost;Port=2136;Database=/local");
await using var connection = await dataSource.OpenConnectionAsync();

await using var cmd = new YdbCommand("SELECT 1;", connection);
await cmd.ExecuteNonQueryAsync();
```

## ADO.NET Example

```csharp
var builder = new YdbConnectionStringBuilder
{
    Host = "server",
    Port = 2135,
    Database = "/my-ydb",
    UseTls = true
};

await using var dataSource = new YdbDataSource(builder);
await using var connection = await dataSource.OpenConnectionAsync();

await using var cmd = connection.CreateCommand();
cmd.CommandText = """
    SELECT series_id, season_id, episode_id, air_date, title
    FROM episodes
    WHERE series_id = @series_id AND season_id > @season_id
    ORDER BY series_id, season_id, episode_id
    LIMIT @limit_size;
    """;
cmd.Parameters.Add(new YdbParameter("series_id", DbType.UInt64, 1U));
cmd.Parameters.Add(new YdbParameter("season_id", DbType.UInt64, 1U));
cmd.Parameters.Add(new YdbParameter("limit_size", DbType.UInt64, 3U));

await using var reader = await cmd.ExecuteReaderAsync();
while (await reader.ReadAsync())
{
    Console.WriteLine($"{reader.GetUint64(0)}, {reader.GetString(4)}");
}
```

## Transactions

```csharp
await using var tx = await connection.BeginTransactionAsync();
await new YdbCommand("UPSERT INTO logs (id, msg) VALUES (1, 'hello');", connection).ExecuteNonQueryAsync();
await tx.CommitAsync();
```

## Retry Policy

Execute operations with automatic retry on transient errors:

```csharp
await dataSource.ExecuteInTransactionAsync(async connection =>
{
    await new YdbCommand("UPSERT INTO ...", connection).ExecuteNonQueryAsync();
});
```

Custom policy:

```csharp
var policy = new YdbRetryPolicy(new YdbRetryPolicyConfig { MaxAttempts = 5 });
var executor = new YdbRetryPolicyExecutor(policy);
await executor.ExecuteAsync(ct => /* your operation */);
```

## Connection String Options

| Option | Default | Description |
|---|---|---|
| `Host` | `localhost` | Server hostname |
| `Port` | `2136` | Server port |
| `Database` | `/local` | Database path |
| `MaxPoolSize` | `100` | Maximum sessions in pool |
| `MinPoolSize` | `0` | Minimum sessions kept alive |
| `SessionIdleTimeout` | `300` | Idle session lifetime (seconds) |
| `CreateSessionTimeout` | `5` | Timeout waiting for a session (seconds) |
| `UseTls` | `false` | Enable TLS |
| `PoolName` | — | Custom pool name (used in metrics) |

## OpenTelemetry

```bash
dotnet add package Ydb.Sdk.OpenTelemetry
```

```csharp
// Tracing
services.AddOpenTelemetry().WithTracing(b => b.AddYdb());

// Metrics
services.AddOpenTelemetry().WithMetrics(b => b.AddYdb());
```

Emitted spans: `ydb.RunWithRetry`, `ydb.Try`, `ydb.ExecuteQuery`, `ydb.Commit`, `ydb.Rollback`, `ydb.CreateSession`.  
Emitted metrics: `db.client.operation.duration`, `ydb.query.session.count`, and more — see [AGENTS.md](../../AGENTS.md).

## More Examples

- [ADO.NET QuickStart](../../examples/Ydb.Sdk.AdoNet.QuickStart)
- [Dapper](../../examples/Ydb.Sdk.AdoNet.Dapper.QuickStart)
- [Yandex Cloud](../../examples/Ydb.Sdk.AdoNet.Yandex.Cloud)
- [OpenTelemetry E2E playground](../../examples/Ydb.Sdk.AdoNet.OpenTelemetry)
- [Topic client](../../examples/Ydb.Sdk.Topic.QuickStart)
- [Changelog](./CHANGELOG.md)
