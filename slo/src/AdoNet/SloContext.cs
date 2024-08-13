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
        var splitEndpoint = config.Endpoint.Split("://");
        var useTls = splitEndpoint[0] switch
        {
            "grpc" => false,
            "grpcs" => true,
            _ => throw new ArgumentException("Don't support schema: " + splitEndpoint[0])
        };

        var host = splitEndpoint[1].Split(":")[0];
        var port = splitEndpoint[1].Split(":")[1];

        return Task.FromResult(new YdbDataSource(new YdbConnectionStringBuilder
            { UseTls = useTls, Host = host, Port = int.Parse(port), Database = config.Db }));
    }
}