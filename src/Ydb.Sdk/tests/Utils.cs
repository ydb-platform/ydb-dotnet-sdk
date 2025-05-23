using System.Runtime.InteropServices;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Tests;

public static class Utils
{
    internal static string Net => RuntimeInformation.FrameworkDescription.Split(".")[1].Split(" ")[1];

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
        var response = await tableClient.SessionExec(async session =>
            await session.ExecuteSchemeQuery(query: query));

        if (ensureSuccess)
        {
            response.Status.EnsureSuccess();
        }

        return (ExecuteSchemeQueryResponse)response;
    }

    internal static ILoggerFactory GetLoggerFactory() =>
        new ServiceCollection()
            .AddLogging(configure =>
            {
                configure.AddConsole().SetMinimumLevel(LogLevel.Information);
                configure.AddFilter("Ydb.Sdk.Ado", LogLevel.Debug);
                configure.AddFilter("Ydb.Sdk.Services.Query", LogLevel.Debug);
                configure.AddFilter("Ydb.Sdk.Services.Topic", LogLevel.Debug);
            })
            .BuildServiceProvider()
            .GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;

    internal static Task CreateSimpleTable(TableClient tableClient, string tableName, string columnName = "key") =>
        ExecuteSchemeQuery(
            tableClient,
            query: $"CREATE TABLE {tableName} ({columnName} Uint64, PRIMARY KEY ({columnName}))");

    internal static Task DropTable(TableClient tableClient, string tableName) =>
        ExecuteSchemeQuery(
            tableClient,
            query: $"DROP TABLE {tableName}");
}
