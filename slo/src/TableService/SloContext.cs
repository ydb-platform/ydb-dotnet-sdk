using Internal.Cli;
using Ydb.Sdk;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Value;

namespace TableService;

public class SloContext : Internal.SloContext<TableClient>
{
    protected override async Task Create(TableClient client, string createTableSql, int operationTimeout)
    {
        var response = await client.SessionExec(
            async session => await session.ExecuteSchemeQuery(createTableSql,
                new ExecuteSchemeQuerySettings { OperationTimeout = TimeSpan.FromSeconds(operationTimeout) }));

        response.Status.EnsureSuccess();
    }

    protected override Task Upsert(string upsertSql, Dictionary<string, YdbValue> parameters, int writeTimeout)
    {
        throw new NotImplementedException();
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