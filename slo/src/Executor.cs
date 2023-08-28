using Prometheus;
using Ydb.Sdk.Table;
using Ydb.Sdk.Value;

namespace slo;

public class Executor
{
    private readonly TableClient _tableClient;

    public Executor(TableClient tableClient)
    {
        _tableClient = tableClient;
    }

    public async Task ExecuteSchemeQuery(string query)
    {
        var response = await _tableClient.SessionExec(
            async session => await session.ExecuteSchemeQuery(query));
        response.Status.EnsureSuccess();
    }

    public async Task<ExecuteDataQueryResponse> ExecuteDataQuery(string query,
        Dictionary<string, YdbValue>? parameters = null, Histogram? histogram = null)
    {
        var txControl = TxControl.BeginSerializableRW().Commit();

        var attempts = 0;
        var response = await _tableClient.SessionExec(
            async session =>
            {
                attempts++;
                return parameters == null
                    ? await session.ExecuteDataQuery(
                        query,
                        txControl)
                    : await session.ExecuteDataQuery(
                        query,
                        txControl,
                        parameters);
            });
        histogram?.WithLabels(response.Status.IsSuccess ? "ok" : "err").Observe(attempts);

        response.Status.EnsureSuccess();

        return (ExecuteDataQueryResponse)response;
    }
}