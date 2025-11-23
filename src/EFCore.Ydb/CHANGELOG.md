- Fixed bug: DateTime.UtcNow generating invalid SQL in ExecuteUpdate ([#555](https://github.com/ydb-platform/ydb-dotnet-sdk/issues/555)).
- Updated Entity Framework Core dependencies from 9.0.0 to 9.0.10 to fix one-to-many relationship Include() runtime errors.

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