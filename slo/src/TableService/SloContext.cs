using Internal;
using Microsoft.Extensions.Logging;
using Prometheus;
using Ydb.Sdk;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Value;

namespace TableService;

public class SloContext : SloContext<TableClient>
{
    private readonly TxControl _txControl = TxControl.BeginSerializableRW().Commit();
    protected override string Job => "workload-table-service";

    protected override async Task Create(TableClient client, string createTableSql, int operationTimeout)
    {
        var response = await client.SessionExec(
            async session => await session.ExecuteSchemeQuery(createTableSql,
                new ExecuteSchemeQuerySettings { OperationTimeout = TimeSpan.FromSeconds(operationTimeout) }));

        response.Status.EnsureSuccess();
    }

    protected override async Task<(int, StatusCode)> Upsert(TableClient tableClient, string upsertSql,
        Dictionary<string, YdbValue> parameters, int writeTimeout, Gauge? errorsGauge = null)
    {
        var querySettings = new ExecuteDataQuerySettings
            { OperationTimeout = TimeSpan.FromSeconds(writeTimeout) };

        var attempts = 0;

        var response = await tableClient.SessionExec(
            async session =>
            {
                attempts++;
                var response = await session.ExecuteDataQuery(upsertSql, _txControl, parameters, querySettings);
                if (response.Status.IsSuccess)
                {
                    return response;
                }


                errorsGauge?.WithLabels(response.Status.StatusCode.ToString(), "retried").Inc();

                return response;
            });

        return (attempts, response.Status.StatusCode);
    }

    protected override async Task<(int, StatusCode, object?)> Select(TableClient tableClient, string selectSql,
        Dictionary<string, YdbValue> parameters, int readTimeout, Gauge? errorsGauge = null)
    {
        var querySettings = new ExecuteDataQuerySettings
            { OperationTimeout = TimeSpan.FromSeconds(readTimeout) };

        var attempts = 0;

        var response = (ExecuteDataQueryResponse)await tableClient.SessionExec(
            async session =>
            {
                attempts++;
                var response = await session.ExecuteDataQuery(selectSql, _txControl, parameters, querySettings);
                if (response.Status.IsSuccess)
                {
                    return response;
                }

                Logger.LogWarning("{}", response.Status.ToString());

                errorsGauge?.WithLabels(response.Status.StatusCode.StatusName(), "retried").Inc();

                return response;
            });

        return (attempts, response.Status.StatusCode,
            response.Status.IsSuccess ? response.Result.ResultSets[0].Rows[0][0].GetOptionalInt32() : null);
    }

    protected override async Task<TableClient> CreateClient(Config config)
    {
        return new TableClient(await Driver.CreateInitialized(new DriverConfig(config.Endpoint, config.Db), Factory));
    }
}