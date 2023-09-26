using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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


    internal static ServiceProvider GetServiceProvider()
    {
        return new ServiceCollection()
            .AddLogging(configure => configure.AddConsole().SetMinimumLevel(LogLevel.Information))
            .BuildServiceProvider();
    }

    internal static ILoggerFactory? GetLoggerFactory()
    {
        var serviceProvider = GetServiceProvider();
        return serviceProvider.GetService<ILoggerFactory>();
    }
}