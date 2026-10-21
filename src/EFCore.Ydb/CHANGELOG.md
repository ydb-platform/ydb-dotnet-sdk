- Added server-side LINQ `TimeOnly.AddHours` translation for tick-backed values, preserving ticks and wrapping across midnight.
- Added server-side LINQ `DateOnly.ToDateTime` translation for dates and tick-backed `TimeOnly` values.
- Fixed LINQ `DateTime.Date` preserving date/time column types and pre-epoch dates for extended-range columns.
- Fixed LINQ `DateTime.AddDays`, `AddHours`, `AddMinutes` and `AddSeconds` translation, including fractional values for `Timestamp` and `Timestamp64`.
- Fixed LINQ `DateTime.AddYears` and `AddMonths` preserving timestamp types and time components, including pre-epoch `Timestamp64` values.
- Added server-side LINQ `DateTime.TimeOfDay` translation for basic and extended-range date/time columns.
- Added server-side LINQ `DateOnly.FromDayNumber` translation across the CLR date range.
- Added server-side LINQ `DateOnly.DayNumber` translation for `Date` and `Date32` columns.
- Added server-side LINQ `DateOnly.FromDateTime` translation, preserving pre-epoch dates for extended-range date/time columns.
- Fixed LINQ `DateOnly.AddYears`, `AddMonths` and `AddDays` translation for `Date` and `Date32` columns.
- Fixed LINQ `DateTime.DayOfWeek` and `DateOnly.DayOfWeek` returning `7` instead of `0` for Sunday.
- Added server-side LINQ `FirstOrDefault` and `LastOrDefault` on strings. Materialization preserves null, empty-string and UTF-16 character semantics; SQL comparisons use YDB `Utf8` text semantics.
- Fixed literal wildcard matching in constant and parameterized LINQ `Contains`, `StartsWith` and `EndsWith`, and preserved backslashes and null characters in generated string literals.
- Fixed LINQ `Contains` with column-based patterns on `Utf8` strings.
- Fixed LINQ `StartsWith` and `EndsWith` with column-based patterns on `Utf8` strings.
- Fixed parameterized `EF.Functions.ILike` returning no matches or being evaluated on the client on EF Core 9 and 10.
- Added string concatenation support in LINQ (`||` in YQL).
- Fixed LINQ `Skip`/`Take` pagination: default `LIMIT` for offset-only queries uses `int.MaxValue` instead of `ulong.MaxValue`.
- Fixed correlated `ExecuteDelete` queries by generating YDB `DELETE ON` from the original join selection.
- Fixed correlated `ExecuteUpdate` queries by generating YDB `UPDATE ON` from the original join selection.
- Dev: `ef-core/{V}` client info is now reported in `x-ydb-sdk-build-info` on every call (previously only on Driver Discovery).
- Bumped `Ydb.Sdk` to `0.33.3`.

## v0.5.0

- Bumped `Ydb.Sdk` to `0.33.0`.
- Started reporting `ef-core/{V}` as client info.
- `YdbExecutionStrategy` now emits the SDK retry telemetry on every `Execute`/`ExecuteAsync`: `ydb.RunWithRetry` outer span, `ydb.Try` per attempt (with `ydb.retry.backoff_ms`), and the `ydb.client.retry.duration` / `ydb.client.retry.attempts` histograms.

## v0.4.2

- Fixed bug: json columns to string property mapping.

## v0.4.1

- Fixed bug: EF Code First now generates correct serial column types for key properties with `ValueGeneratedOnAdd`:
  - `Int64` -> `Bigserial`
  - `Int32` -> `Serial`
  - `Int16` -> `SmallSerial`
- Fixed bug: `HasDefaultValue(...)` / `HasDefaultValueSql(...)` no longer cause incorrect serial mapping for `ValueGeneratedOnAdd` columns.

## v0.4.0

- Upgraded ADO.NET provider version: `0.26.0` → `0.27.0`.

## v0.3.0

- Added support for `Microsoft.EntityFrameworkCore` 10.0 on .NET 10.0.
- Added `ILike` support.
- Added support `INSERT`, `UPDATE`, `DELETE` batching for EF Core SaveChanges operations.
- Fixed bug: DateTime.UtcNow generating invalid SQL in ExecuteUpdate ([#555](https://github.com/ydb-platform/ydb-dotnet-sdk/issues/555)).

## v0.2.0

- Fixed bug: Missing/removed __EFMigrationsHistory caused INSERT errors (LockMigration); migrations now handle table recreation and locking properly ([#544](https://github.com/ydb-platform/ydb-dotnet-sdk/issues/544)).
- Added support for new YDB data types - `Date32`, `Datetime64`, `Timestamp64` and `Interval64`.
- Fixed bug: Missing `DEFAULT` clause generation in `YdbMigrationsSqlGenerator` (handles defaultValue/defaultValueSql) ([#552](https://github.com/ydb-platform/ydb-dotnet-sdk/issues/552)).
- Fixed bug: SqlQuery throws exception when using list parameters ([#540](https://github.com/ydb-platform/ydb-dotnet-sdk/issues/540)).
- Added support for the YDB retry policy (ADO.NET) and new configuration methods in `YdbDbContextOptionsBuilder`:
  - `EnableRetryIdempotence()`: enables retries for errors classified as idempotent. You must ensure the operation itself is idempotent.
  - `UseRetryPolicy(YdbRetryPolicyConfig retryPolicyConfig)`: configures custom backoff parameters and the maximum number of retry attempts.

## v0.1.0

- Fixed bug: incompatible coalesce types ([#531](https://github.com/ydb-platform/ydb-dotnet-sdk/issues/531)).
- Upgraded ADO.NET provider version: `0.17.0` → `0.24.0`.
- Fixed Decimal precision/scale mapping in EF provider.
- Supported Guid (Uuid YDB type).
- PrivateAssets="none" is set to flow the EF Core analyzer to users referencing this package [issue](https://github.com/aspnet/EntityFrameworkCore/pull/11350).

## v0.0.2

- Fixed bug: Decimal type forces EF migrator to create migrations again and again ([#434](https://github.com/ydb-platform/ydb-dotnet-sdk/issues/434)).

## v0.0.1

- Supported migration tools: initial implementation of EF Core migration tool support.
- Enabled `EntityFrameworkCore.FunctionalTests` for unit and functional testing coverage.
- Supported connections to Yandex Cloud (YDB Cloud) using standard configuration options.
- First provider implementation for YDB: basic CRUD, DbContext mapping, and initial LINQ translation functionality.
