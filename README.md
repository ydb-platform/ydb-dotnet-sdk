[![Nuget](https://img.shields.io/nuget/v/Ydb.Sdk)](https://www.nuget.org/packages/Ydb.Sdk/)

# YDB .NET SDK
YDB client libraries for .NET.

## Prerequisites
.NET 6 or .NET 7

## Versioning

We follow the **[SemVer 2.0.0](https://semver.org)**. In particular, we provide backward compatibility in the `MAJOR` releases. New features without loss of backward compatibility appear on the `MINOR` release. In the minor version, the patch number starts from `0`. Bug fixes and internal changes are released with the third digit (`PATCH`) in the version.

Major version zero (`0.y.z`) is considered prerelease and **do not guarantee any backward compatibility**.

## Installation

```
dotnet add package Ydb.Sdk
```

## Usage

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

### Credentials
YDB SDK provides several standard ways for authentication:
1) `Ydb.Sdk.Auth.AnonymousProvider`. Anonymous YDB access, mainly for tests purposes.
2) `Ydb.Sdk.Auth.TokenProvider`. Token authentication for OAuth-like tokens.
3) `Ydb.Sdk.Auth.StaticCredentialsProvider`. Username and password based authentication.

For Yandex.Cloud specific authentication methods, consider using **[ydb-dotnet-yc](https://github.com/ydb-platform/ydb-dotnet-yc)**.

### TableClient
After you have driver instance, you can use it to create clients for different YDB services. The straightforward example of querying data may look similar to the following example:

```c#
// Create Ydb.Sdk.Table.TableClient using Driver instance.
using var tableClient = new TableClient(driver, new TableClientConfig());

// Execute operation on arbitrary session with default retry policy
var response = await tableClient.SessionExec(async session =>
{
    var query = @"
        DECLARE $id AS Uint64;

        SELECT
            series_id,
            title,
            release_date
        FROM series
        WHERE series_id = $id;
    ";

    return await session.ExecuteDataQuery(
        query: query,
        parameters: new Dictionary<string, YdbValue>
        {
            { "$id", YdbValue.MakeUint64(id) }
        },
        // Begin serializable transaction and commit automatically after query execution
        txControl: TxControl.BeginSerializableRW().Commit(),
    );
});

response.Status.EnsureSuccess();

var queryResponse = (ExecuteDataQueryResponse)response;
var resultSet = queryResponse.Result.ResultSets[0];

foreach (var row in resultSet.Rows)
{
    Console.WriteLine($"> Series, " +
        $"series_id: {(ulong?)row["series_id"]}, " +
        $"title: {(string?)row["title"]}, " +
        $"release_date: {(DateTime?)row["release_date"]}");
}
```

## Examples

See **[ydb-dotnet-examples](https://github.com/ydb-platform/ydb-dotnet-examples)**.
