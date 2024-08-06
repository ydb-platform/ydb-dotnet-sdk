using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Tests;

public static class Utils
{
    public static async Task<ExecuteDataQueryResponse> ExecuteDataQuery(TableClient tableClient, string query,
        Dictionary<string, YdbValue>? parameters = null)
    {
        var response = await tableClient.SessionExec(async session =>
            parameters != null
                ? await session.ExecuteDataQuery(
                    query: query,
                    txControl: TxControl.BeginSerializableRW().Commit(),
                    parameters: parameters
                )
                : await session.ExecuteDataQuery(
                    query: query,
                    txControl: TxControl.BeginSerializableRW().Commit()));

        response.Status.EnsureSuccess();
        return (ExecuteDataQueryResponse)response;
    }

    public static async Task<ExecuteSchemeQueryResponse> ExecuteSchemeQuery(
        TableClient tableClient, string query, bool ensureSuccess = true)
    {
        var response = await tableClient.SessionExec(
            async session =>
                await session.ExecuteSchemeQuery(query: query));

        if (ensureSuccess)
        {
            response.Status.EnsureSuccess();
        }

        return (ExecuteSchemeQueryResponse)response;
    }

    internal static ILoggerFactory GetLoggerFactory()
    {
        return new ServiceCollection()
            .AddLogging(configure => configure.AddConsole().SetMinimumLevel(LogLevel.Debug))
            .BuildServiceProvider()
            .GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
    }

    internal static async Task CreateSimpleTable(TableClient tableClient, string tableName, string columnName = "key")
    {
        await ExecuteSchemeQuery(
            tableClient,
            query: $"CREATE TABLE {tableName} ({columnName} Uint64, PRIMARY KEY ({columnName}))");
    }

    internal static async Task DropTable(TableClient tableClient, string tableName)
    {
        await ExecuteSchemeQuery(
            tableClient,
            query: $"DROP TABLE {tableName}");
    }
}
