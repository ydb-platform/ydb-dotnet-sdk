using Internal.Cli;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Value;

namespace AdoNet;

public class SloContext : Internal.SloContext<YdbDataSource>
{
    protected override async Task Create(YdbDataSource client, string createTableSql, int operationTimeout)
    {
        await using var ydbConnection = await client.OpenConnectionAsync();

        await new YdbCommand(ydbConnection)
        {
            CommandText = createTableSql,
            CommandTimeout = operationTimeout
        }.ExecuteNonQueryAsync();
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

    public override Task<YdbDataSource> CreateClient(Config config)
    {
        var endpointWithoutSchema = config.Endpoint.Split("://")[1];
        var hostAndPort = endpointWithoutSchema.Split(":");

        return Task.FromResult(new YdbDataSource(new YdbConnectionStringBuilder
            { Host = hostAndPort[0], Port = int.Parse(hostAndPort[1]), Database = config.Db }));
    }
}