using Microsoft.Extensions.Logging;
using OpenTelemetry.Logs;
using OpenTelemetry.Resources;
using Ydb.Sdk.Ado;

const string serviceName = "ydb-sdk-otel-logs-sample";
var otlpEndpoint = new Uri("http://otel-collector:4317");

var serviceVersion = typeof(Program).Assembly.GetName().Version?.ToString() ?? "unknown";

var resourceBuilder = ResourceBuilder.CreateDefault()
    .AddService(serviceName, serviceVersion: serviceVersion);

// The SDK writes its logs through whatever ILoggerFactory you hand it. Here we build
// a factory backed by the OpenTelemetry logging provider and export every log record
// to the collector via OTLP. From there the collector forwards them to Loki.
using var loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddOpenTelemetry(options =>
    {
        options.SetResourceBuilder(resourceBuilder);
        options.AddOtlpExporter(o => o.Endpoint = otlpEndpoint);
    });
});

var logger = loggerFactory.CreateLogger("Sample");

logger.LogInformation("[{Time:u}] started, service.name={ServiceName}", DateTimeOffset.UtcNow, serviceName);

// Pass the factory to the SDK: every internal log (driver init, session pool, query
// execution, retries, ...) is now exported to the collector as an OTLP log record.
await using var dataSource = new YdbDataSource(
    new YdbConnectionStringBuilder("Host=ydb;Port=2136;Database=/local")
    {
        LoggerFactory = loggerFactory,
        PoolName = "logs-sample"
    });

logger.LogInformation("Initializing schema...");

await using (var connInit = await dataSource.OpenConnectionAsync())
{
    await new YdbCommand("CREATE TABLE IF NOT EXISTS bank_logs(id Int32, amount Int32, PRIMARY KEY (id))", connInit)
        .ExecuteNonQueryAsync();
    await new YdbCommand("UPSERT INTO bank_logs(id, amount) VALUES (1, 0)", connInit).ExecuteNonQueryAsync();
}

logger.LogInformation("Running a few transactions...");

for (var i = 0; i < 5; i++)
{
    await dataSource.ExecuteInTransactionAsync(async ydbConnection =>
    {
        var count = (int)(await new YdbCommand(ydbConnection)
            { CommandText = "SELECT amount FROM bank_logs WHERE id = 1" }.ExecuteScalarAsync())!;

        await new YdbCommand(ydbConnection)
        {
            CommandText = "UPDATE bank_logs SET amount = @amount + 1 WHERE id = 1",
            Parameters = { new YdbParameter { Value = count, ParameterName = "amount" } }
        }.ExecuteNonQueryAsync();
    });
}

logger.LogInformation("Generating an error to show a failed-query log...");

try
{
    await using var errConn = await dataSource.OpenConnectionAsync();
    await new YdbCommand("SELECT * FROM table_that_does_not_exist_xyz", errConn).ExecuteNonQueryAsync();
}
catch (YdbException e)
{
    logger.LogError(e, "Query failed as expected with status {StatusCode}", e.Code);
}

logger.LogInformation("App finished. Waiting 15s to flush logs...");
await Task.Delay(15000);