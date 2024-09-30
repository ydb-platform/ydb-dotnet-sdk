[![Nuget](https://img.shields.io/nuget/v/Ydb.Sdk)](https://www.nuget.org/packages/Ydb.Sdk/)

# YDB .NET SDK
Provides an ADO.NET standard implementation for working with YDB, as well as native clients for lightweight interaction with YDB.

## Prerequisites
.NET 6 or above

## Features

- **ADO.NET**: Full support for standard ADO.NET interfaces including DbConnection, DbCommand, DbDataReader, and more. This allows you to use familiar methods and patterns for database operations while leveraging the power and flexibility of YDB.
- **QueryClient**: A lightweight, high-performance native client for direct interaction with YDB tables.

## Versioning

We follow the **[SemVer 2.0.0](https://semver.org)**. In particular, we provide backward compatibility in the `MAJOR` releases. New features without loss of backward compatibility appear on the `MINOR` release. In the minor version, the patch number starts from `0`. Bug fixes and internal changes are released with the third digit (`PATCH`) in the version.

Major version zero (`0.y.z`) is considered prerelease and **do not guarantee any backward compatibility**.

## Installation

```
dotnet add package Ydb.Sdk
```

## Usage ADO.NET

Example of using ADO.NET to execute a SQL query against YDB:

```c#
var ydbConnectionBuilder = new YdbConnectionStringBuilder
{
    Host = "server",
    Port = 2135,
    Database = "/my-ydb",
    UseTls = true,
    CredentialsProvider = credentialsProvider // Credentials provider, see "Credentials" section
};

await using var connection = new YdbConnection();
await connection.OpenAsync();

var ydbCommand = connection.CreateCommand();
ydbCommand.CommandText = """
                         SELECT series_id, season_id, episode_id, air_date, title
                         FROM episodes
                         WHERE series_id = @series_id AND season_id > @season_id
                         ORDER BY series_id, season_id, episode_id
                         LIMIT @limit_size;
                         """;
ydbCommand.Parameters.Add(new YdbParameter("$series_id", DbType.UInt64, 1U));
ydbCommand.Parameters.Add(new YdbParameter("$season_id", DbType.UInt64, 1U));
ydbCommand.Parameters.Add(new YdbParameter("$limit_size", DbType.UInt64, 3U));

var ydbDataReader = await ydbCommand.ExecuteReaderAsync();

_logger.LogInformation("Selected rows:");
while (await ydbDataReader.ReadAsync())
{
    _logger.LogInformation(
        "series_id: {series_id}, season_id: {season_id}, episode_id: {episode_id}, air_date: {air_date}, title: {title}",
        ydbDataReader.GetUint64(0), ydbDataReader.GetUint64(1), ydbDataReader.GetUint64(2),
        ydbDataReader.GetDateTime(3), ydbDataReader.GetString(4));
}
```

## Usage Native clients

To begin your work with YDB, create an instance of `Ydb.Sdk.Driver` class:
```c#
var config = new DriverConfig(
    endpoint: endpoint, // Database endpoint, "grpcs://host:port"
    database: database, // Full database path
    credentials: credentialsProvider // Credentials provider, see "Credentials" section
);

using var driver = new Driver(
    config: config,
    loggerFactory: loggerFactory
);

await driver.Initialize(); // Make sure to await driver initialization
```

After you have driver instance, you can use it to create clients for different YDB services.

### Credentials
YDB SDK provides several standard ways for authentication:
1) `Ydb.Sdk.Auth.AnonymousProvider`. Anonymous YDB access, mainly for tests purposes.
2) `Ydb.Sdk.Auth.TokenProvider`. Token authentication for OAuth-like tokens.
3) `Ydb.Sdk.Auth.StaticCredentialsProvider`. Username and password based authentication.

For Yandex.Cloud specific authentication methods, consider using **[ydb-dotnet-yc](https://github.com/ydb-platform/ydb-dotnet-yc)**.

### QueryClient

Example of using a query client to execute a simple query:

```c#
// Create QueryClient using Driver instance.
using var queryClient = new QueryService(driver);

var row = await queryClient.ReadRow(@"
        DECLARE $id AS Uint64;

        SELECT
            series_id,
            title,
            release_date
        FROM series
        WHERE series_id = $id;
    ",
    new Dictionary<string, YdbValue>
    {
        { "$id", YdbValue.MakeUint64(id) }
    }
);

Console.WriteLine($"> Series, " +
    $"series_id: {(ulong?)row["series_id"]}, " +
    $"title: {(string?)row["title"]}, " +
    $"release_date: {(DateTime?)row["release_date"]}");
```

## Examples

See **[examples folder](https://github.com/ydb-platform/ydb-dotnet-sdk/tree/main/examples)**
