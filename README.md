# YDB .NET Ecosystem

[![Telegram](https://img.shields.io/badge/Telegram-Русский_чат-2ba2d9.svg?logo=telegram)](https://t.me/ydb_ru)
[![Telegram](https://img.shields.io/badge/Telegram-English_chat-2ba2d9.svg?logo=telegram)](https://t.me/ydb_en)
[![WebSite](https://img.shields.io/badge/website-ydb.tech-blue.svg)](https://ydb.tech)

## Overview

This repository contains all official .NET libraries for working with [YDB](https://ydb.tech) —
a distributed SQL database by Yandex. All packages are developed, tested, and released from this
single monorepo.

- **Ydb.Sdk** — Core SDK: ADO.NET provider, Topic (Pub/Sub) client, gRPC transport, session pooling, retry policies.
- **EntityFrameworkCore.Ydb** — Entity Framework Core provider built on top of `Ydb.Sdk`.
- **Ydb.Sdk.OpenTelemetry** — OpenTelemetry instrumentation: tracing and metrics extensions.

## Packages

| Package | NuGet | README | Documentation |
|---|---|---|---|
| `Ydb.Sdk` | [![NuGet](https://img.shields.io/nuget/v/Ydb.Sdk)](https://www.nuget.org/packages/Ydb.Sdk) | [README](./src/Ydb.Sdk/README.md) | [ADO.NET](https://ydb.tech/docs/en/reference/languages-and-apis/ado-net), [Topic](https://ydb.tech/docs/en/reference/ydb-sdk/topic) |
| `EntityFrameworkCore.Ydb` | [![NuGet](https://img.shields.io/nuget/v/EntityFrameworkCore.Ydb)](https://www.nuget.org/packages/EntityFrameworkCore.Ydb) | [README](./src/EFCore.Ydb/README.md) | [EF Core](https://ydb.tech/docs/en/integrations/orm/entity-framework?version=main) |
| `Ydb.Sdk.OpenTelemetry` | [![NuGet](https://img.shields.io/nuget/v/Ydb.Sdk.OpenTelemetry)](https://www.nuget.org/packages/Ydb.Sdk.OpenTelemetry) | [README](./src/Ydb.Sdk.OpenTelemetry/README.md) | — |

## Quick Start

```bash
dotnet add package Ydb.Sdk
```

```csharp
await using var dataSource = new YdbDataSource("Host=localhost;Port=2136;Database=/local");
await using var connection = await dataSource.OpenConnectionAsync();

await using var cmd = new YdbCommand("SELECT 1;", connection);
await cmd.ExecuteNonQueryAsync();
```

## Examples

Ready-to-run example projects live in the [`examples/`](./examples) folder:

| Example | Description |
|---|---|
| [AdoNet.QuickStart](./examples/Ydb.Sdk.AdoNet.QuickStart) | Minimal ADO.NET usage |
| [AdoNet.Dapper.QuickStart](./examples/Ydb.Sdk.AdoNet.Dapper.QuickStart) | Dapper integration |
| [AdoNet.Yandex.Cloud](./examples/Ydb.Sdk.AdoNet.Yandex.Cloud) | Connecting to YDB in Yandex Cloud |
| [AdoNet.OpenTelemetry](./examples/Ydb.Sdk.AdoNet.OpenTelemetry) | Full OTel stack: Grafana + Tempo + Prometheus |
| [Topic.QuickStart](./examples/Ydb.Sdk.Topic.QuickStart) | Topic Pub/Sub client |
| [EntityFrameworkCore.QuickStart](./examples/EntityFrameworkCore.Ydb.QuickStart) | EF Core basics |
| [EntityFrameworkCore.Samples](./examples/EntityFrameworkCore.Ydb.Samples) | EF Core — LINQ queries, relationships |
| [Linq2db.QuickStart](./examples/Linq2db.QuickStart) | Linq2db integration |

## Versioning

This project follows **[SemVer 2.0.0](https://semver.org)**:
- `MAJOR` — breaking changes
- `MINOR` — new features, backward compatible
- `PATCH` — bug fixes and internal improvements

> Major version zero (`0.y.z`) is pre-release and does **not** guarantee backward compatibility.

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](./CONTRIBUTING.md) for guidelines.

## License

Licensed under the [Apache 2.0 License](./LICENSE).
