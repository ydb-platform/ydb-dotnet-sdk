using System.Diagnostics.Metrics;
using Internal;
using Internal.Cli;
using Prometheus;
using Ydb.Sdk;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Value;

namespace TableService;

public class SloContext : SloContext<TableClient>
{
    protected override async Task Create(TableClient client, string createTableSql, int operationTimeout)
    {
        var response = await client.SessionExec(
            async session => await session.ExecuteSchemeQuery(createTableSql,
                new ExecuteSchemeQuerySettings { OperationTimeout = TimeSpan.FromSeconds(operationTimeout) }));

        response.Status.EnsureSuccess();
    }

    protected override async Task<int> Upsert(TableClient tableClient, string upsertSql,
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
                Console.WriteLine(response.Status);

                return response;
            });

        response.Status.EnsureSuccess();

        return attempts;
    }

    protected override Task<string> Select(string selectSql, Dictionary<string, YdbValue> parameters, int readTimeout)
    {
        throw new NotImplementedException();
    }

    protected override Task CleanUp(string dropTableSql, int operationTimeout)
    {
        throw new NotImplementedException();
    }

    public override async Task<TableClient> CreateClient(Config config)
    {
        return new TableClient(await Driver.CreateInitialized(new DriverConfig(config.Endpoint, config.Db)));
    }
}