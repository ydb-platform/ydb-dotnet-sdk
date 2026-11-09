# YDB .NET SDK — Repository Guide for AI Agents

## Overview

This is the **official YDB .NET SDK monorepo**. It contains all .NET client libraries for
[YDB](https://ydb.tech) — a distributed SQL database by Yandex. All packages are developed,
tested, and released from this single repository.

**Published NuGet packages:**

| Package                   | Description                                                           |
|---------------------------|-----------------------------------------------------------------------|
| `Ydb.Sdk`                 | Core client: ADO.NET provider, Topic (Pub/Sub) client, gRPC transport |
| `EntityFrameworkCore.Ydb` | Entity Framework Core provider built on top of `Ydb.Sdk`              |
| `Ydb.Sdk.OpenTelemetry`   | OpenTelemetry instrumentation (tracing + metrics extension methods)   |

---

## Repository Layout

```
/
├── src/
│   ├── Ydb.Sdk/                    # Core SDK
│   │   ├── CHANGELOG.md            # Release notes (see "Conventions")
│   │   ├── src/                    # Library source (net8.0) — Ydb.Sdk.csproj
│   │   └── test/
│   │       ├── Ydb.Sdk.Ado.Tests/              # ADO.NET integration tests
│   │       ├── Ydb.Sdk.Ado.Specification.Tests/ # ADO.NET spec compliance
│   │       ├── Ydb.Sdk.Ado.Dapper.Tests/       # Dapper integration tests
│   │       ├── Ydb.Sdk.Ado.Benchmarks/         # BenchmarkDotNet benchmarks
│   │       ├── Ydb.Sdk.Ado.Stress.Loader/      # Stress testing tool
│   │       └── Ydb.Sdk.Topic.Tests/            # Topic client tests
│   │
│   ├── EFCore.Ydb/                 # Entity Framework Core provider
│   │   ├── CHANGELOG.md
│   │   ├── src/                    # Library source (net8.0 → EF 9, net10.0 → EF 10)
│   │   └── test/EntityFrameworkCore.Ydb.FunctionalTests/  # net9.0;net10.0
│   │
│   ├── Ydb.Sdk.OpenTelemetry/      # OpenTelemetry package
│   │   ├── CHANGELOG.md
│   │   └── src/                    # AddYdb / AddYdbAdo / AddYdbTopic extensions
│   │
│   └── YdbSdk.sln                  # Main solution (src + test projects only)
│
├── examples/                       # Standalone example applications
│   ├── YdbExamples.sln             # Examples-only solution
│   ├── Ydb.Sdk.AdoNet.QuickStart/
│   ├── Ydb.Sdk.AdoNet.Dapper.QuickStart/
│   ├── Ydb.Sdk.AdoNet.Yandex.Cloud/
│   ├── Ydb.Sdk.AdoNet.Yandex.Cloud.Serverless.Container/
│   ├── Ydb.Sdk.AdoNet.WebProxy/    # Connecting through an HTTP proxy (squid)
│   ├── Ydb.Sdk.OpenTelemetry/      # OTel usage for AdoNet/ and Topics/
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
└── slo/                            # Service Level Objectives testing (see slo/README.md)
    └── src/                        # src.sln
        ├── AdoNet/                 # ADO.NET SLO app (has Dockerfile)
        ├── Dapper/
        ├── EF/
        ├── Linq2db/
        ├── TopicService/           # Topic SLO app
        └── Internal/               # Shared SLO utilities (CLI, OTLP metrics, workload harness)
```

> **Important:** `examples/` and `slo/` are intentionally **excluded from `YdbSdk.sln`**.
> They have their own solution files (`examples/YdbExamples.sln`, `slo/src/src.sln`).
> CI lints/formats each of the three solutions separately, so a change in `examples/` or
> `slo/` must be formatted against *its own* solution.

---

## Source Code Structure (`src/Ydb.Sdk/src/`)

### ADO.NET Provider (`Ado/`)

The primary public API. Implements standard `System.Data` abstractions for YDB.

| Class                        | Role                                                                                  |
|------------------------------|---------------------------------------------------------------------------------------|
| `YdbConnection`              | `DbConnection` implementation; manages a session from the pool                        |
| `YdbCommand`                 | `DbCommand` implementation; executes YQL queries                                      |
| `YdbDataReader`              | `DbDataReader` implementation; streams result rows                                    |
| `YdbDataSource`              | `DbDataSource` implementation; owns the session pool and is the preferred entry point |
| `YdbTransaction`             | `DbTransaction` implementation; wraps YDB interactive transactions                    |
| `YdbParameter`               | `DbParameter` implementation; typed query parameters                                  |
| `YdbConnectionStringBuilder` | Parses and validates connection strings                                               |
| `YdbException`               | Exception type carrying a `StatusCode` from YDB                                       |

#### Session Pool (`Ado/Session/`)

| Class                     | Role                                                                                                                                              |
|---------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `PoolingSessionSource<T>` | Fixed-size session pool. Uses a `ConcurrentStack<T>` for idle sessions, `ConcurrentQueue<TCS>` for waiters, and a `T?[]` array for slot tracking. |
| `ImplicitSessionSource`   | No-pool mode: each `OpenSession()` call creates a new server-managed session.                                                                     |
| `PoolingSessionBase<T>`   | Base class for pooled sessions; manages state (`In`/`Out`/`Clean`) via `Interlocked` CAS.                                                         |
| `PoolingSessionState`     | Enum: `In` = idle in pool, `Out` = in use, `Clean` = closed/removed.                                                                              |

Key design notes:

- `Statistics` (Idle/Busy) is computed by scanning the `_sessions[]` array and counting sessions
  in `In` state, then clamping with `Math.Min(idle, total)`. This avoids negative values that
  would occur if `CleanIdleSessions` decrements `_numSessions` before removing dead sessions
  from the `ConcurrentStack`.
- `IdleStartTime` uses `DateTime.UtcNow` (not `DateTime.Now`) to avoid DST-related jumps.
- `ObjectDisposedException` is a property (not a field) so it is only instantiated when actually thrown.

#### Retry Policy (`Ado/RetryPolicy/`)

| Class                    | Role                                                                                                |
|--------------------------|-----------------------------------------------------------------------------------------------------|
| `IRetryPolicy`           | Interface: `TimeSpan? GetNextDelay(YdbException, int attempt)`                                      |
| `YdbRetryPolicy`         | Default implementation. Exponential backoff with jitter (AWS pattern).                              |
| `YdbRetryPolicyConfig`   | Configuration: `MaxAttempts`, base/cap delays for fast/slow paths, idempotence flag.                |
| `YdbRetryPolicyExecutor` | Wraps an `IRetryPolicy` and an async operation. Emits `ydb.RunWithRetry` / `ydb.Try` tracing spans. |

Retry delay strategy by status code:

| Status Code(s)                                   | Strategy                    |
|--------------------------------------------------|-----------------------------|
| `BadSession`, `SessionBusy`, `SessionExpired`    | Immediate (`TimeSpan.Zero`) |
| `Aborted`, `Undetermined`                        | Fast backoff + full jitter  |
| `Unavailable`, transport errors                  | Fast backoff + equal jitter |
| `Overloaded`, `ClientTransportResourceExhausted` | Slow backoff + equal jitter |
| Anything else                                    | No retry (`null`)           |

#### Metrics (`YdbMetricsReporter`)

One instance per session pool. Meter name: **`Ydb.Sdk`**.

All `ydb.query.session.*` metrics include the attribute **`ydb.query.session.pool.name`** (from the `PoolName` connection string option; defaults to the full connection string).

| Metric                               | Kind            | Unit          | Attributes                                                                 | Description                                                                                                               |
|--------------------------------------|-----------------|---------------|----------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|
| `ydb.client.operation.duration`      | Histogram       | `s`           | `endpoint`, `database`, `operation.name`                                   | Latency of each actual attempt for `ExecuteQuery`, `Commit`, or `Rollback`.                                               |
| `ydb.client.operation.failed`        | Counter         | `{operation}` | `endpoint`, `database`,`operation.name`, `status_code`                     | Number of unsuccessful operation attempts.                                                                                |
| `ydb.query.session.create_time`      | Histogram       | `s`           | `ydb.query.session.pool.name`                                              | Cost of creating a session (CreateSession + first AttachStream message).                                                  |
| `ydb.query.session.pending_requests` | Counter         | `{request}`   | `ydb.query.session.pool.name`                                              | Increments when a request starts waiting for a free session; use **rate** to see wait pressure (not current queue depth). |
| `ydb.query.session.timeouts`         | Counter         | `{timeout}`   | `ydb.query.session.pool.name`                                              | Pool could not serve demand in time (session acquisition timeouts).                                                       |
| `ydb.query.session.count`            | ObservableGauge | `{session}`   | `ydb.query.session.pool.name`, `ydb.query.session.state` (`idle` / `used`) | Current pool state.                                                                                                       |
| `ydb.query.session.max`              | ObservableGauge | `{session}`   | `ydb.query.session.pool.name`                                              | Context: configured `MaxPoolSize`.                                                                                        |
| `ydb.query.session.min`              | ObservableGauge | `{session}`   | `ydb.query.session.pool.name`                                              | Context: configured `MinPoolSize`.                                                                                        |

`database` is the YDB database path; `endpoint` is `host:port` from the connection string.

#### Tracing (`Tracing/YdbActivitySource`)

ActivitySource name: **`Ydb.Sdk`**.

| Span                    | Kind     | When emitted                                                                            |
|-------------------------|----------|-----------------------------------------------------------------------------------------|
| `ydb.Driver.Initialize` | Internal | Driver first initialization                                                             |
| `ydb.CreateSession`     | Client   | Session creation (gRPC call)                                                            |
| `ydb.RunWithRetry`      | Internal | Wraps the full retry loop in `YdbRetryPolicyExecutor`                                   |
| `ydb.Try`               | Internal | One per attempt (including the first); each attempt's `operation()` call runs inside it |
| `ydb.ExecuteQuery`      | Client   | Individual YQL query execution                                                          |
| `ydb.Commit`            | Client   | Transaction commit                                                                      |
| `ydb.Rollback`          | Client   | Transaction rollback                                                                    |

Span lifecycle for `ydb.RunWithRetry` / `ydb.Try`:

- `ydb.RunWithRetry` is the outer span for the whole operation.
- `ydb.Try` is created for every attempt. The first attempt's span has no extra tags.
- Retry attempt spans carry the `ydb.retry.backoff_ms` tag (the backoff waited before that attempt).
- On non-retryable error: the current `ydb.Try` and `ydb.RunWithRetry` both get error status.
- On retries exhausted: all `ydb.Try` spans and `ydb.RunWithRetry` get error status.
- On cancellation: `error.type` is set to the exception type full name on the affected spans.

Error tags set by `SetException(Exception)`:

- For `YdbException`: `db.response.status_code` — the YDB `StatusCode`; `error.type` — `"transport_error"` for client
  transport codes, `"ydb_error"` otherwise
- For other exceptions (e.g. `OperationCanceledException`): `error.type` — the full exception type name

### Topic Client (`Topic/`)

Pub/Sub client for YDB Topics (similar to Kafka). Key classes: `TopicClient`, `TopicReader<T>`,
`TopicWriter<T>`. Supports custom serializers/deserializers. Tests are in `Ydb.Sdk.Topic.Tests`.

### Authentication (`Auth/`)

| Provider                            | Description                                                            |
|-------------------------------------|------------------------------------------------------------------------|
| `TokenProvider`                     | Static token                                                           |
| `CachedCredentialsProvider`         | Wraps any provider and caches the token until expiry                   |
| `ServiceAccountCredentialsProvider` | Yandex.Cloud service account JWT (reads `.json` key file)              |
| Metadata credentials                | Yandex.Cloud VM/Container metadata service (set via connection string) |

### Transport (`Transport/`, `Pool/`, `Services/`)

- gRPC over HTTP/2 using Grpc.Net.Client.
- `DirectGrpcChannelDriver` — single-endpoint driver.
- `BalancingGrpcChannelDriver` — multi-endpoint driver with server-side discovery.
- `EndpointPool` + `ChannelPool` — manage gRPC channels per endpoint.

---

## Key Design Decisions

1. **OpenTelemetry sources are split only where selective subscription is useful.** Tracing and ADO metrics use
   `Ydb.Sdk`; Topic metrics use `Ydb.Sdk.Topic`. `MeterProviderBuilder.AddYdb()` subscribes to both meters;
   `AddYdbAdo()` and `AddYdbTopic()` allow selective metric subscription.

2. **Example and SLO projects are not in `YdbSdk.sln`** — each has its own solution and is
   formatted/inspected by CI independently. Examples and SLO apps reference the SDK via
   `ProjectReference`, so they always build against the current source.

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

Requires .NET SDK 10 (EF Core provider targets `net10.0`; CI lint uses 10.0.x).

```bash
# Build the main solution
dotnet build src/YdbSdk.sln

# Start a local YDB (integration tests need it; same flags as CI)
docker run -d --rm --name ydb-local -h localhost \
  -p 2135:2135 -p 2136:2136 -p 8765:8765 \
  -e YDB_LOCAL_SURVIVE_RESTART=true \
  -e YDB_FEATURE_FLAGS=enable_parameterized_decimal,enable_table_datetime64 \
  ydbplatform/local-ydb:latest
docker cp ydb-local:/ydb_certs/ca.pem ~/      # TLS tests expect ~/ca.pem

# Run one test project / one test
dotnet test src/Ydb.Sdk/test/Ydb.Sdk.Ado.Tests/
dotnet test src/Ydb.Sdk/test/Ydb.Sdk.Ado.Tests/ --filter "FullyQualifiedName~YdbCommandTests"

# EF Core functional tests (CI runs net9.0)
dotnet test src/EFCore.Ydb/test/EntityFrameworkCore.Ydb.FunctionalTests/ -f net9.0
```

- ADO tests read the connection string from `YDB_TEST_DB`, default
  `Host=localhost;Port=2136;Database=local;MaxPoolSize=10` (`Ydb.Sdk.Ado.Tests/Utils/TestUtils.cs`).
- Proxy tests in `Ydb.Sdk.Ado.Tests` additionally need a squid proxy on `:3128`
  (config in `Ydb.Sdk.Ado.Tests/Proxy/`; see `tests.yml` for the exact `docker run`).
- `Ydb.Sdk.Ado.Benchmarks` and `Ydb.Sdk.Ado.Stress.Loader` are executables, not test suites.
- Test stack: xUnit 2.x + Moq. ADO tests inherit `TestBase` (helpers such as
  `CreateOpenConnectionAsync`, `RunTestWithTemporaryTable`).

### Formatting and inspections

CI does **not** use `dotnet format`. It runs JetBrains ReSharper CLI:

```bash
dotnet tool install -g JetBrains.ReSharper.GlobalTools --version 2025.3.2
cd src && jb cleanupcode YdbSdk.sln --profile="Custom Cleanup"
# examples:  cd examples && jb cleanupcode YdbExamples.sln --profile="Custom Cleanup"
# slo:       cd slo/src  && jb cleanupcode src.sln        --profile="Custom Cleanup"
```

The cleanup profile lives in `*.sln.DotSettings`. After cleanup CI fails on any `git diff`.
A second job runs `jb inspectcode` with minimum severity **WARNING** — new warnings fail the build.

---

## CI/CD (`.github/workflows/`)

| Workflow              | Trigger                       | Purpose                                                                                  |
|-----------------------|-------------------------------|------------------------------------------------------------------------------------------|
| `tests.yml`           | PR / push to main / hourly    | SDK tests (YDB `latest`+`trunk` × .NET 8/9), EF tests, runs examples; Codecov upload     |
| `lint.yml`            | PR / push to main             | ReSharper `cleanupcode` diff check + `inspectcode` for all three solutions               |
| `check-slo-label.yml` | PR                            | PR must carry the `SLO` label (or `no slo` to skip)                                      |
| `slo.yml`             | PR                            | SLO for AdoNet / Dapper / EF / Linq2db via `ydb-platform/ydb-slo-action`                 |
| `slo-topic.yaml`      | PR                            | SLO for TopicService                                                                     |
| `slo-report.yml`      | after SLO workflows           | Publishes SLO report                                                                     |
| `publish.yml`         | Manual (PATCH/MINOR, RC flag) | Release `Ydb.Sdk` (tag `vX.Y.Z`)                                                         |
| `publish-ef.yml`      | Manual (PATCH/MINOR, RC flag) | Release `EntityFrameworkCore.Ydb` (tag `ef-vX.Y.Z`)                                      |
| `publish-otel.yml`    | Manual (explicit version)     | Release `Ydb.Sdk.OpenTelemetry`                                                          |
| `codeql-analysis.yml` | PR / push / weekly            | Security analysis                                                                        |

The hourly `tests.yml` schedule exists to catch flaky tests — avoid timing-sensitive assertions.

---

## Conventions

- **Commits / PR titles:** Conventional Commits with a scope, e.g. `fix(ado): …`, `feat(efcore): …`,
  `feat(topic): …`, `fix(slo): …`, `feat(opentelemetry): …`. PRs are squash-merged with `(#NNN)`.
- **CHANGELOG is mandatory for user-visible changes.** Add a bullet at the **top** of the relevant
  `CHANGELOG.md`, above the first `## vX.Y.Z` header:
  - `Ydb.Sdk` / `EFCore.Ydb`: no "Unreleased" header — release scripts take everything above the
    first `## v` line and insert the version header themselves. Style: `- Fix ADO.NET: …`,
    `- Feat Topic Reader metrics: …`, `- **Breaking Change** ADO.NET: …` (SDK);
    `- Fixed …` / `- Added …` (EF).
  - `Ydb.Sdk.OpenTelemetry`: entries go under `## Unreleased`; the version header is written by hand.
- **EF Core provider depends on the published `Ydb.Sdk` NuGet package** (see
  `EntityFrameworkCore.Ydb.csproj`), not on the project. EF tests reference the SDK project, so a
  provider change that needs a new SDK API compiles in tests but not in the package until the SDK is
  released and the version is bumped.
- **EF 9 vs EF 10:** `net8.0` builds against EF Core 9 with `EFCORE9` defined, `net10.0` against
  EF Core 10. Version-specific code uses `#if EFCORE9` or `*.Ef9.cs` / `*.Ef10.cs` files excluded in the csproj.
- Library projects use `Nullable` + `ImplicitUsings`.
- `tmp/`, `generated-images/`, `.omo/` are local scratch — do not commit them.

### Before you finish a change

- Build `src/YdbSdk.sln` and run the affected test project against a local YDB (see "Build and Test").
  If YDB is not running, say so — don't silently skip tests.
- Run `jb cleanupcode` (see "Formatting and inspections") on every solution you touched.
- Add a CHANGELOG bullet for every user-visible change.
- When you add or rename a meter, instrument, attribute or span, update the metrics/tracing tables in
  this file in the same change.
- Don't touch `publish*.yml`, `.github/scripts/publish*.sh` or version numbers unless asked.

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
