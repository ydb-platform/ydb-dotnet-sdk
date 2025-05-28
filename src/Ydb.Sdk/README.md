# YDB .NET SDK

[![NuGet](https://img.shields.io/nuget/v/Ydb.Sdk)](https://www.nuget.org/packages/Ydb.Sdk)

## Overview

Provides an ADO.NET standard implementation for working with YDB, as well as native clients for lightweight interaction with YDB.

## Features

- **Ydb.Sdk.Ado**: Full support for standard ADO.NET interfaces including DbConnection, DbCommand, DbDataReader, and more. This allows you to use familiar methods and patterns for database operations while leveraging the power and flexibility of YDB.
- **Ydb.Sdk.Services.Topic**: Writer and Reader topic clients implement core topic functionality for YDB – writing and reading message streams.

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
    UseTls = true
};

await using var connection = new YdbConnection(ydbConnectionBuilder);
await connection.OpenAsync();

var ydbCommand = connection.CreateCommand();
ydbCommand.CommandText = """
                         SELECT series_id, season_id, episode_id, air_date, title
                         FROM episodes
                         WHERE series_id = @series_id AND season_id > @season_id
                         ORDER BY series_id, season_id, episode_id
                         LIMIT @limit_size;
                         """;
ydbCommand.Parameters.Add(new YdbParameter("series_id", DbType.UInt64, 1U));
ydbCommand.Parameters.Add(new YdbParameter("season_id", DbType.UInt64, 1U));
ydbCommand.Parameters.Add(new YdbParameter("limit_size", DbType.UInt64, 3U));

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

## More examples

- [AdoNet simple guide](./../../examples/Ydb.Sdk.AdoNet.QuickStart)
- [AdoNet connect to Yandex Cloud](./../../examples/Ydb.Sdk.AdoNet.Yandex.Cloud)
- [Dapper example](./../../examples/Ydb.Sdk.AdoNet.Dapper.QuickStart)
- [Topic client](./../../examples/Ydb.Sdk.Topic.QuickStart)
