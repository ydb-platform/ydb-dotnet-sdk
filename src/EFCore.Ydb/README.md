# EntityFrameworkCore.Ydb

[![NuGet](https://img.shields.io/nuget/v/EntityFrameworkCore.Ydb)](https://www.nuget.org/packages/EntityFrameworkCore.Ydb)

Entity Framework Core provider for [YDB](https://ydb.tech) — a distributed SQL database by Yandex.
Built on top of the `Ydb.Sdk` ADO.NET provider.

## Installation

```bash
dotnet add package EntityFrameworkCore.Ydb
```

## Supported versions

| Package | Supported EF Core version | Target framework |
|---|---|---|
| `EntityFrameworkCore.Ydb` | 9.x | net8.0 |
| `EntityFrameworkCore.Ydb` | 10.x | net10.0 |

## Quick Start

### 1. Define your model

```csharp
public class AppDbContext : DbContext
{
    public DbSet<Series> Series { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseYdb("Host=localhost;Port=2136;Database=/local");
}

public class Series
{
    public ulong SeriesId { get; set; }
    public string Title { get; set; } = "";
    public DateTime ReleaseDate { get; set; }
}
```

### 2. Create and apply migrations

```bash
dotnet tool install --global dotnet-ef
dotnet ef migrations add InitialCreate
dotnet ef database update
```

### 3. Query data

```csharp
await using var db = new AppDbContext();

var recent = await db.Series
    .Where(s => s.ReleaseDate.Year >= 2020)
    .OrderBy(s => s.Title)
    .ToListAsync();
```

## Features

- Full EF Core migration support (create / alter / drop tables)
- LINQ query translation to YQL
- Transactions (serializable and snapshot read-only)
- Retry policy integration via `UseYdb(..., options => options.UseRetryPolicy(...))`
- Data types: `string`, `bool`, integers, `float`/`double`, `DateTime`, `DateOnly`, `TimeSpan`, `Guid`, `Json`
- Extended YDB types: `Date32`, `Datetime64`, `Timestamp64`, `Interval64`
- Serial columns (auto-increment via `SERIAL`)
- `INSERT`/`UPDATE`/`DELETE` batching
- One-to-one, one-to-many, many-to-many relationships
- Yandex Cloud authentication (service account key, metadata service)

## Yandex Cloud

```csharp
options.UseYdb(
    "Host=<endpoint>;Port=2135;Database=<db>;UseTls=true;ServiceAccountKeyFilePath=/path/to/key.json"
);
```

## Documentation

- [Official EF Core docs for YDB](https://ydb.tech/docs/en/integrations/orm/entity-framework?version=main)
- [EF Core QuickStart example](../../examples/EntityFrameworkCore.Ydb.QuickStart)
- [EF Core advanced samples](../../examples/EntityFrameworkCore.Ydb.Samples)
- [Changelog](./CHANGELOG.md)
