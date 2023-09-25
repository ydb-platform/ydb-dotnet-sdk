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

    public static async Task<ExecuteSchemeQueryResponse> ExecuteSchemeQuery(TableClient tableClient, string query)
    {
        var response = await tableClient.SessionExec(
            async session =>
                await session.ExecuteSchemeQuery(query: query));

        response.Status.EnsureSuccess();
        return (ExecuteSchemeQueryResponse)response;
    }
}