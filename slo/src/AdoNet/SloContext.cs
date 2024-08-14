using Internal;
using Internal.Cli;
using Polly;
using Prometheus;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Value;

namespace AdoNet;

public class SloContext : SloContext<YdbDataSource>
{
    private readonly AsyncPolicy _policy = Policy.Handle<YdbException>()
        .WaitAndRetryAsync(10, attempt => TimeSpan.FromSeconds(attempt),
            (_, _, retryCount, context) => { context["RetryCount"] = retryCount; });

    protected override async Task Create(YdbDataSource client, string createTableSql, int operationTimeout)
    {
        await using var ydbConnection = await client.OpenConnectionAsync();

        await new YdbCommand(ydbConnection)
                { CommandText = createTableSql, CommandTimeout = operationTimeout }
            .ExecuteNonQueryAsync();
    }

    protected override async Task<int> Upsert(YdbDataSource dataSource, string upsertSql,
        Dictionary<string, YdbValue> parameters, int writeTimeout, Gauge? errorsGauge = null)
    {
        var policyResult = await _policy.ExecuteAndCaptureAsync(async () =>
        {
            await using var ydbConnection = await dataSource.OpenConnectionAsync();

            var ydbCommand = new YdbCommand(ydbConnection)
                { CommandText = upsertSql, CommandTimeout = writeTimeout };

            foreach (var (key, value) in parameters)
            {
                ydbCommand.Parameters.AddWithValue(key, value);
            }

            await ydbCommand.ExecuteNonQueryAsync();
        });

        return (int)policyResult.Context["RetryCount"];
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