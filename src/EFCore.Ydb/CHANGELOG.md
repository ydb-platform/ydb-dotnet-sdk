- Fixed Decimal precision/scale mapping in EF provider and added functional tests.
- Supported Guid (Uuid YDB type).
- PrivateAssets="none" is set to flow the EF Core analyzer to users referencing this package [issue](https://github.com/aspnet/EntityFrameworkCore/pull/11350).

## v0.0.2

- Fixed bug: Decimal type forces EF migrator to create migrations again and again ([#434](https://github.com/ydb-platform/ydb-dotnet-sdk/issues/434)).

## v0.0.1

- Supported migration tools: initial implementation of EF Core migration tool support.
- Enabled `EntityFrameworkCore.FunctionalTests` for unit and functional testing coverage.
- Supported connections to Yandex Cloud (YDB Cloud) using standard configuration options.
- First provider implementation for YDB: basic CRUD, DbContext mapping, and initial LINQ translation functionality.