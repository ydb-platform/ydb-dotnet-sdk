using Internal;
using Internal.Cli;
using Microsoft.Extensions.Logging;
using Prometheus;
using Ydb.Sdk;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Value;

namespace TableService;

public class SloContext : SloContext<TableClient>
{
    protected override string JobName => "workload-table-service";

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
        var txControl = TxControl.BeginSerializableRW().Commit();

        var querySettings = new ExecuteDataQuerySettings
            { OperationTimeout = TimeSpan.FromSeconds(writeTimeout) };

        var attempts = 0;

        var response = await tableClient.SessionExec(
            async session =>
            {
                attempts++;
                var response = await session.ExecuteDataQuery(upsertSql, txControl, parameters, querySettings);
                if (response.Status.IsSuccess)
                {
                    return response;
                }

                errorsGauge?.WithLabels(Utils.GetResonseStatusName(response.Status.StatusCode), "retried").Inc();

                return response;
            });

        return (attempts, response.Status.StatusCode);
    }

    protected override Task<(int, StatusCode, object)> Select(TableClient client, string selectSql,
        Dictionary<string, YdbValue> parameters, int readTimeout, Gauge? errorsGauge = null)
    {
        throw new NotImplementedException();
    }

    protected override async Task<TableClient> CreateClient(Config config)
    {
        return new TableClient(await Driver.CreateInitialized(new DriverConfig(config.Endpoint, config.Db), Factory));
    }
}