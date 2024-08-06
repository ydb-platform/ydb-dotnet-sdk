using Microsoft.Extensions.Logging;
using slo.Cli;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Value;

namespace slo.AdoNet;

internal class SloContext : slo.SloContext
{
    private YdbConnectionStringBuilder Builder { get; }

    internal SloContext(Config config, ILoggerFactory factory) : base(factory.CreateLogger<SloContext>())
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

        Builder = new YdbConnectionStringBuilder
        {
            UseTls = useTls,
            Host = host,
            Port = int.Parse(port),
            LoggerFactory = factory
        };
    }

    protected override async Task Create(string createTableSql, int operationTimeout)
    {
        await using var ydbConnection = new YdbConnection(Builder);
        var ydbCommand = ydbConnection.CreateCommand();
        ydbCommand.CommandText = createTableSql;
        ydbCommand.CommandTimeout = operationTimeout;

        await ydbCommand.ExecuteNonQueryAsync();
    }

    protected override async Task Upsert(string upsertSql, Dictionary<string, YdbValue> parameters, int writeTimeout)
    {
        await using var ydbConnection = new YdbConnection(Builder);
        var ydbCommand = new YdbCommand(ydbConnection)
        {
            CommandText = upsertSql,
            CommandTimeout = writeTimeout
        };

        foreach (var (key, value) in parameters)
        {
            ydbCommand.Parameters.AddWithValue(key, value);
        }

        await ydbCommand.ExecuteNonQueryAsync();
    }

    protected override async Task<string> Select(string selectSql, Dictionary<string, YdbValue> parameters,
        int readTimeout)
    {
        await using var ydbConnection = new YdbConnection(Builder);
        var ydbCommand = new YdbCommand(ydbConnection)
        {
            CommandText = selectSql,
            CommandTimeout = readTimeout
        };

        foreach (var (key, value) in parameters)
        {
            ydbCommand.Parameters.AddWithValue(key, value);
        }

        return (string)(await ydbCommand.ExecuteScalarAsync())!;
    }

    protected override async Task CleanUp(string dropTableSql, int operationTimeout)
    {
        await using var ydbConnection = new YdbConnection(Builder);
        var ydbCommand = new YdbCommand(ydbConnection)
        {
            CommandText = dropTableSql,
            CommandTimeout = operationTimeout
        };

        await ydbCommand.ExecuteNonQueryAsync();
    }
}