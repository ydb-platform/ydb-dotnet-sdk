using Internal;
using Microsoft.Extensions.Logging;
using Polly;
using Prometheus;
using Ydb.Sdk;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Value;

namespace AdoNet;

public class SloTableContext : SloTableContext<YdbDataSource>
{
    private readonly AsyncPolicy _policy = Policy.Handle<YdbException>(exception => exception.IsTransient)
        .WaitAndRetryAsync(10, attempt => TimeSpan.FromMilliseconds(attempt * 10),
            (e, _, _, context) =>
            {
                var errorsTotal = (Counter)context["errorsTotal"];

                Logger.LogWarning(e, "Failed read / write operation");
                errorsTotal?.WithLabels(((YdbException)e).Code.StatusName(), "retried").Inc();
            });

    protected override string Job => "AdoNet";

    protected override async Task Create(YdbDataSource client, string createTableSql, int operationTimeout)
    {
        await using var ydbConnection = await client.OpenConnectionAsync();

        await new YdbCommand(ydbConnection)
                { CommandText = createTableSql, CommandTimeout = operationTimeout }
            .ExecuteNonQueryAsync();
    }

    protected override async Task<(int, StatusCode)> Upsert(YdbDataSource dataSource, string upsertSql,
        Dictionary<string, YdbValue> parameters, int writeTimeout, Counter? errorsTotal = null)
    {
        var context = new Context();
        if (errorsTotal != null)
        {
            context["errorsTotal"] = errorsTotal;
        }

        var policyResult = await _policy.ExecuteAndCaptureAsync(async _ =>
        {
            await using var ydbConnection = await dataSource.OpenConnectionAsync();

            var ydbCommand = new YdbCommand(ydbConnection)
                { CommandText = upsertSql, CommandTimeout = writeTimeout };

            foreach (var (key, value) in parameters)
            {
                ydbCommand.Parameters.AddWithValue(key, value);
            }

            await ydbCommand.ExecuteNonQueryAsync();
        }, context);


        return (policyResult.Context.TryGetValue("RetryCount", out var countAttempts) ? (int)countAttempts : 1,
            ((YdbException)policyResult.FinalException)?.Code ?? StatusCode.Success);
    }

    protected override async Task<(int, StatusCode, object?)> Select(YdbDataSource dataSource, string selectSql,
        Dictionary<string, YdbValue> parameters, int readTimeout, Counter? errorsTotal = null)
    {
        var context = new Context();
        if (errorsTotal != null)
        {
            context["errorsTotal"] = errorsTotal;
        }

        var attempts = 0;
        var policyResult = await _policy.ExecuteAndCaptureAsync(async _ =>
        {
            attempts++;
            await using var ydbConnection = await dataSource.OpenConnectionAsync();

            var ydbCommand = new YdbCommand(ydbConnection)
                { CommandText = selectSql, CommandTimeout = readTimeout };

            foreach (var (key, value) in parameters)
            {
                ydbCommand.Parameters.AddWithValue(key, value);
            }

            return await ydbCommand.ExecuteScalarAsync();
        }, context);

        return (attempts, ((YdbException)policyResult.FinalException)?.Code ?? StatusCode.Success, policyResult.Result);
    }

    protected override Task<YdbDataSource> CreateClient(Config config)
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
        {
            UseTls = useTls, Host = host, Port = int.Parse(port), Database = config.Db,
            LoggerFactory = ISloContext.Factory
        }));
    }
}