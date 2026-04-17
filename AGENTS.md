# YDB .NET SDK — Repository Guide for AI Agents

## Overview

This is the **official YDB .NET SDK monorepo**. It contains all .NET client libraries for
[YDB](https://ydb.tech) — a distributed SQL database by Yandex. All packages are developed,
tested, and released from this single repository.

**Published NuGet packages:**

| Package | Description |
|---|---|
| `Ydb.Sdk` | Core client: ADO.NET provider, Topic (Pub/Sub) client, gRPC transport |
| `EntityFrameworkCore.Ydb` | Entity Framework Core provider built on top of `Ydb.Sdk` |
| `Ydb.Sdk.OpenTelemetry` | OpenTelemetry instrumentation (tracing + metrics extension methods) |

---

## Repository Layout

```
/
├── src/
│   ├── Ydb.Sdk/                    # Core SDK
│   │   ├── src/Ydb.Sdk/            # Library source (net8.0)
│   │   └── test/
│   │       ├── Ydb.Sdk.Ado.Tests/              # ADO.NET integration tests
│   │       ├── Ydb.Sdk.Ado.Specification.Tests/ # ADO.NET spec compliance
│   │       ├── Ydb.Sdk.Ado.Dapper.Tests/       # Dapper integration tests
│   │       ├── Ydb.Sdk.Ado.Benchmarks/         # BenchmarkDotNet benchmarks
│   │       ├── Ydb.Sdk.Ado.Stress.Loader/      # Stress testing tool
│   │       └── Ydb.Sdk.Topic.Tests/            # Topic client tests
│   │
│   ├── EFCore.Ydb/                 # Entity Framework Core provider
│   │   ├── src/EntityFrameworkCore.Ydb/        # Library source (net8.0, net10.0)
│   │   └── test/EntityFrameworkCore.Ydb.FunctionalTests/
│   │
│   ├── Ydb.Sdk.OpenTelemetry/      # OpenTelemetry package
│   │   └── src/Ydb.Sdk.OpenTelemetry/
│   │
│   └── YdbSdk.sln                  # Main solution (src + test projects only)
│
├── examples/                       # Standalone example applications
│   ├── YdbExamples.sln             # Examples-only solution
│   ├── Ydb.Sdk.AdoNet.QuickStart/
│   ├── Ydb.Sdk.AdoNet.Dapper.QuickStart/
│   ├── Ydb.Sdk.AdoNet.Yandex.Cloud/
│   ├── Ydb.Sdk.AdoNet.Yandex.Cloud.Serverless.Container/
│   ├── Ydb.Sdk.AdoNet.OpenTelemetry/  # Full OTel E2E playground (Docker Compose)
│   │   ├── Trace/                  # Tracing example app
│   │   ├── Metrics/                # Metrics load-tank example app
│   │   └── grafana/dashboards/     # Pre-built Grafana dashboards
│   ├── Ydb.Sdk.Topic.QuickStart/
│   ├── EntityFrameworkCore.Ydb.QuickStart/
│   ├── EntityFrameworkCore.Ydb.Samples/
│   ├── EntityFrameworkCore.Ydb.Yandex.Cloud/
│   └── Linq2db.QuickStart/
│
└── slo/                            # Service Level Objectives testing
    └── src/
        ├── AdoNet/                 # ADO.NET SLO app (has Dockerfile)
        ├── TopicService/           # Topic SLO app
        ├── Linq2db/
        └── Internal/               # Shared SLO utilities
```

> **Important:** `examples/` and `slo/` are intentionally **excluded from `YdbSdk.sln`**.
> They have their own solution files. This keeps the main solution clean and prevents
> code formatters/linters from touching example code when run against `YdbSdk.sln`.

---

## Source Code Structure (`src/Ydb.Sdk/src/Ydb.Sdk/`)

### ADO.NET Provider (`Ado/`)

The primary public API. Implements standard `System.Data` abstractions for YDB.

| Class | Role |
|---|---|
| `YdbConnection` | `DbConnection` implementation; manages a session from the pool |
| `YdbCommand` | `DbCommand` implementation; executes YQL queries |
| `YdbDataReader` | `DbDataReader` implementation; streams result rows |
| `YdbDataSource` | `DbDataSource` implementation; owns the session pool and is the preferred entry point |
| `YdbTransaction` | `DbTransaction` implementation; wraps YDB interactive transactions |
| `YdbParameter` | `DbParameter` implementation; typed query parameters |
| `YdbConnectionStringBuilder` | Parses and validates connection strings |
| `YdbException` | Exception type carrying a `StatusCode` from YDB |

#### Session Pool (`Ado/Session/`)

| Class | Role |
|---|---|
| `PoolingSessionSource<T>` | Fixed-size session pool. Uses a `ConcurrentStack<T>` for idle sessions, `ConcurrentQueue<TCS>` for waiters, and a `T?[]` array for slot tracking. |
| `ImplicitSessionSource` | No-pool mode: each `OpenSession()` call creates a new server-managed session. |
| `PoolingSessionBase<T>` | Base class for pooled sessions; manages state (`In`/`Out`/`Clean`) via `Interlocked` CAS. |
| `PoolingSessionState` | Enum: `In` = idle in pool, `Out` = in use, `Clean` = closed/removed. |

Key design notes:
- `Statistics` (Idle/Busy) is computed by scanning the `_sessions[]` array and counting sessions
  in `In` state, then clamping with `Math.Min(idle, total)`. This avoids negative values that
  would occur if `CleanIdleSessions` decrements `_numSessions` before removing dead sessions
  from the `ConcurrentStack`.
- `IdleStartTime` uses `DateTime.UtcNow` (not `DateTime.Now`) to avoid DST-related jumps.
- `ObjectDisposedException` is a property (not a field) so it is only instantiated when actually thrown.

#### Retry Policy (`Ado/RetryPolicy/`)

| Class | Role |
|---|---|
| `IRetryPolicy` | Interface: `TimeSpan? GetNextDelay(YdbException, int attempt)` |
| `YdbRetryPolicy` | Default implementation. Exponential backoff with jitter (AWS pattern). |
| `YdbRetryPolicyConfig` | Configuration: `MaxAttempts`, base/cap delays for fast/slow paths, idempotence flag. |
| `YdbRetryPolicyExecutor` | Wraps an `IRetryPolicy` and an async operation. Emits `ydb.RunWithRetry` / `ydb.Try` tracing spans. |

Retry delay strategy by status code:

| Status Code(s) | Strategy |
|---|---|
| `BadSession`, `SessionBusy`, `SessionExpired` | Immediate (`TimeSpan.Zero`) |
| `Aborted`, `Undetermined` | Fast backoff + full jitter |
| `Unavailable`, transport errors | Fast backoff + equal jitter |
| `Overloaded`, `ClientTransportResourceExhausted` | Slow backoff + equal jitter |
| Anything else | No retry (`null`) |

#### Metrics (`YdbMetricsReporter`)

One instance per session pool. Meter name: **`Ydb.Sdk`**.

| Metric | Kind | Tags | Description |
|---|---|---|---|
| `db.client.operation.duration` | Histogram (seconds) | `db.system.name`, `db.namespace`, `server.address`, `server.port`, `ydb.operation.name` | Latency of ADO.NET operations (ExecuteQuery, Commit, Rollback) |
| `ydb.client.operation.failed` | Counter | `ydb.operation.name`, `db.response.status_code` | Count of failed operations |
| `ydb.query.session.count` | ObservableUpDownCounter | `ydb.query.session.pool.name`, `ydb.query.session.state` (`idle`/`used`) | Current pool session counts |
| `ydb.query.session.create_time` | Histogram (seconds) | `ydb.query.session.pool.name` | Time to create a new session (RPC + first message) |
| `ydb.query.session.pending_requests` | UpDownCounter | `ydb.query.session.pool.name` | Requests waiting for a session |
| `ydb.query.session.timeouts` | Counter | `ydb.query.session.pool.name` | Timed-out connection acquisitions |

#### Tracing (`Tracing/YdbActivitySource`)

ActivitySource name: **`Ydb.Sdk`**.

| Span | Kind | When emitted |
|---|---|---|
| `ydb.Driver.Initialize` | Internal | Driver first initialization |
| `ydb.CreateSession` | Client | Session creation (gRPC call) |
| `ydb.RunWithRetry` | Internal | Wraps the full retry loop in `YdbRetryPolicyExecutor` |
| `ydb.Try` | Internal | One per attempt (including the first); each attempt's `operation()` call runs inside it |
| `ydb.ExecuteQuery` | Client | Individual YQL query execution |
| `ydb.Commit` | Client | Transaction commit |
| `ydb.Rollback` | Client | Transaction rollback |

Span lifecycle for `ydb.RunWithRetry` / `ydb.Try`:
- `ydb.RunWithRetry` is the outer span for the whole operation.
- `ydb.Try` is created for every attempt. The first attempt's span has no extra tags.
- Retry attempt spans carry the `ydb.retry.backoff_ms` tag (the backoff waited before that attempt).
- On non-retryable error: the current `ydb.Try` and `ydb.RunWithRetry` both get error status.
- On retries exhausted: all `ydb.Try` spans and `ydb.RunWithRetry` get error status.
- On cancellation: `error.type` is set to the exception type full name on the affected spans.

Error tags set by `SetException(Exception)`:
- For `YdbException`: `db.response.status_code` — the YDB `StatusCode`; `error.type` — `"transport_error"` for client transport codes, `"ydb_error"` otherwise
- For other exceptions (e.g. `OperationCanceledException`): `error.type` — the full exception type name

### Topic Client (`Topic/`)

Pub/Sub client for YDB Topics (similar to Kafka). Key classes: `TopicClient`, `TopicReader<T>`,
`TopicWriter<T>`. Supports custom serializers/deserializers. Tests are in `Ydb.Sdk.Topic.Tests`.

### Authentication (`Auth/`)

| Provider | Description |
|---|---|
| `TokenProvider` | Static token |
| `CachedCredentialsProvider` | Wraps any provider and caches the token until expiry |
| `ServiceAccountCredentialsProvider` | Yandex.Cloud service account JWT (reads `.json` key file) |
| Metadata credentials | Yandex.Cloud VM/Container metadata service (set via connection string) |

### Transport (`Transport/`, `Pool/`, `Services/`)

- gRPC over HTTP/2 using Grpc.Net.Client.
- `DirectGrpcChannelDriver` — single-endpoint driver.
- `BalancingGrpcChannelDriver` — multi-endpoint driver with server-side discovery.
- `EndpointPool` + `ChannelPool` — manage gRPC channels per endpoint.

---

## Key Design Decisions

1. **Single activity source** (`Ydb.Sdk`) for both tracing and metrics. Consumers opt-in via
   `AddYdb()` extension methods on `TracerProviderBuilder` / `MeterProviderBuilder`.

2. **Example projects are not in `YdbSdk.sln`** — this prevents the CI auto-formatter
   (`dotnet format`) from touching example files and causing spurious diffs.

3. **`PoolingSessionSource` uses a fixed-size `T?[]` array** as the authoritative session
   registry (not the `ConcurrentStack`), because `ConcurrentStack` can contain stale references
   to sessions already closed by `CleanIdleSessions`.

4. **`ydb.Try` spans one attempt each**. The backoff delay is inside next span `ydb.Try`. 
   Child RPC spans are correctly parented inside the `ydb.Try` of the attempt that produced them.

5. **`EnableImplicitSession`** switches from `PoolingSessionSource` to `ImplicitSessionSource`,
   which relies on server-managed sessions. Useful for write-heavy workloads that don't benefit
   from client-side pooling.

---

## Build and Test

```bash
# Build the main solution
dotnet build src/YdbSdk.sln

# Run all tests (requires a running YDB instance)
dotnet test src/YdbSdk.sln

# Run specific test project
dotnet test src/Ydb.Sdk/test/Ydb.Sdk.Ado.Tests/

# Format code (run from src/ to avoid touching examples)
dotnet format src/YdbSdk.sln
```

Test configuration uses environment variables or `appsettings.json` for the YDB connection
string. The CI pipeline starts a local YDB via Docker.

---

## CI/CD (`.github/workflows/`)

| Workflow | Trigger | Purpose |
|---|---|---|
| `tests.yml` | PR / push | Run integration tests against local YDB |
| `lint.yml` | PR / push | `dotnet format --verify-no-changes` + other linters |
| `publish.yml` | Manual | Publish `Ydb.Sdk` to NuGet |
| `publish-ef.yml` | Manual | Publish `EntityFrameworkCore.Ydb` to NuGet |
| `codeql-analysis.yml` | Schedule / PR | Security analysis |
| `slo.yml` | Manual / schedule | SLO reliability tests |

The lint workflow runs `dotnet format` against `YdbSdk.sln` and compares the result to the
committed files. Any uncommitted formatting diff causes CI to fail. Always format before pushing.

---

## OpenTelemetry E2E Playground (`examples/Ydb.Sdk.AdoNet.OpenTelemetry/`)

A full Docker Compose stack for local observability testing:

- **otel-collector** — receives OTLP on `:4317`, exposes Prometheus scrape on `:9464`
- **Tempo** — distributed trace backend
- **Prometheus** — scrapes metrics from otel-collector
- **Grafana** — pre-configured dashboards in `grafana/dashboards/ydb-metrics.json`
- **Trace app** (`Trace/`) — demonstrates tracing spans
- **Metrics app** (`Metrics/`) — load-tank with phased RPS pattern (Peak → Medium → Min → Medium)

```bash
cd examples/Ydb.Sdk.AdoNet.OpenTelemetry
docker compose up --build
```

---

## Connection String Reference

```
Host=localhost;Port=2136;Database=/local;
MaxPoolSize=100;MinPoolSize=0;
CreateSessionTimeout=5;SessionIdleTimeout=300;
UseTls=false;
PoolName=my-pool
```

Full list of options: see `YdbConnectionStringBuilder` in
`src/Ydb.Sdk/src/Ado/YdbConnectionStringBuilder.cs`.
