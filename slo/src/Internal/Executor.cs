using Prometheus;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Value;

namespace Internal;

public class Executor(TableClient tableClient)
{
    public async Task ExecuteSchemeQuery(string query, TimeSpan? timeout = null)
    {
        var response = await tableClient.SessionExec(
            async session => await session.ExecuteSchemeQuery(query,
                new ExecuteSchemeQuerySettings { OperationTimeout = timeout, TransportTimeout = timeout * 1.1 }));
        response.Status.EnsureSuccess();
    }

    public async Task<ExecuteDataQueryResponse> ExecuteDataQuery(
        string query,
        Dictionary<string, YdbValue>? parameters = null,
        TimeSpan? timeout = null,
        Histogram? attemptsHistogram = null,
        Gauge? errorsGauge = null)

    {
        var txControl = TxControl.BeginSerializableRW().Commit();

        var querySettings = new ExecuteDataQuerySettings
            { OperationTimeout = timeout, TransportTimeout = timeout * 1.1 };

        var attempts = 0;

        var response = await tableClient.SessionExec(
            async session =>
            {
                attempts++;
                var response = parameters == null
                    ? await session.ExecuteDataQuery(
                        query,
                        txControl,
                        querySettings)
                    : await session.ExecuteDataQuery(
                        query,
                        txControl,
                        parameters,
                        querySettings);
                if (response.Status.IsSuccess)
                {
                    return response;
                }
                errorsGauge?.WithLabels(Utils.GetResonseStatusName(response.Status.StatusCode), "retried").Inc();
                Console.WriteLine(response.Status);

                return response;
            });
        attemptsHistogram?.WithLabels(response.Status.IsSuccess ? "ok" : "err").Observe(attempts);
        if (!response.Status.IsSuccess)
        {
            errorsGauge?.WithLabels(Utils.GetResonseStatusName(response.Status.StatusCode), "finally").Inc();
        }

        response.Status.EnsureSuccess();

        return (ExecuteDataQueryResponse)response;
    }
}