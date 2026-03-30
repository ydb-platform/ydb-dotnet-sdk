using System.Diagnostics;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Ydb.Sdk.Ado;
using Ydb.Sdk.AdoNet.OpenTelemetry;
using Ydb.Sdk.OpenTelemetry;

if (args.Length > 0 && args[0] == "--load")
{
    await LoadGenerator.Run();
    return;
}

const string serviceName = "ydb-sdk-sample";
var otlpEndpoint = new Uri("http://otel-collector:4317");

var serviceVersion = typeof(Program).Assembly.GetName().Version?.ToString() ?? "unknown";

var resourceBuilder = ResourceBuilder.CreateDefault()
    .AddService(serviceName, serviceVersion: serviceVersion);

const string activitySourceName = "Ydb.Sdk.AdoNet.OpenTelemetry.Sample";
using var activitySource = new ActivitySource(activitySourceName);

using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .SetResourceBuilder(resourceBuilder)
    .AddSource(activitySourceName)
    .AddYdb()
    .AddOtlpExporter(o => { o.Endpoint = otlpEndpoint; })
    .Build();

using var meterProvider = Sdk.CreateMeterProviderBuilder()
    .SetResourceBuilder(resourceBuilder)
    .AddRuntimeInstrumentation()
    .AddYdb()
    .AddOtlpExporter(o => { o.Endpoint = otlpEndpoint; })
    .Build();

Console.WriteLine($"[{DateTimeOffset.UtcNow:u}] started, service.name={serviceName}");

Console.WriteLine("Initializing...");

await using var dataSource = new YdbDataSource("Host=ydb;Port=2136;Database=/local");
const string appStartup = "app.startup";
var currentDataSource = dataSource;

using (_ = activitySource.StartActivity(appStartup))
{
    await using var connInit = await dataSource.OpenConnectionAsync();
    try
    {
        await new YdbCommand("DROP TABLE bank", connInit).ExecuteNonQueryAsync();
    }
    catch
    {
        // ignored
    }

    await new YdbCommand("CREATE TABLE bank(id Int32, amount Int32, PRIMARY KEY (id))", connInit)
        .ExecuteNonQueryAsync();
}

Console.WriteLine("Insert row...");

await using var connInsertRow = await dataSource.OpenConnectionAsync();
await new YdbCommand("INSERT INTO bank(id, amount) VALUES (1, 0)", connInsertRow).ExecuteNonQueryAsync();

Console.WriteLine("Preparing queries...");
await dataSource.ExecuteInTransactionAsync(async ydbConnection =>
{
    var count = (int)(await new YdbCommand(ydbConnection)
        { CommandText = "SELECT amount FROM bank WHERE id = 1" }.ExecuteScalarAsync())!;

    await new YdbCommand(ydbConnection)
    {
        CommandText = "UPDATE bank SET amount = @amount + 1 WHERE id = 1",
        Parameters = { new YdbParameter { Value = count, ParameterName = "amount" } }
    }.ExecuteNonQueryAsync();
});

Console.WriteLine("Emulation TLI...");

var tasks = new List<Task>();
for (var i = 0; i < 10; i++)
    tasks.Add(Task.Run(async () =>
    {
        for (var j = 0; j < 5; j++)
            try
            {
                await currentDataSource.ExecuteInTransactionAsync(async ydbConnection =>
                {
                    var count = (int)(await new YdbCommand(ydbConnection)
                        { CommandText = "SELECT amount FROM bank WHERE id = 1" }.ExecuteScalarAsync())!;

                    await new YdbCommand(ydbConnection)
                    {
                        CommandText = "UPDATE bank SET amount = @amount + 1 WHERE id = 1",
                        Parameters = { new YdbParameter { Value = count, ParameterName = "amount" } }
                    }.ExecuteNonQueryAsync();
                });
            }
            catch
            {
                // ignored
            }
    }));

await Task.WhenAll(tasks);

Console.WriteLine("Retry connection example...");

await using var ydbConnection = await dataSource.OpenRetryableConnectionAsync();

await new YdbCommand(ydbConnection)
    { CommandText = "SELECT amount FROM bank WHERE id = 1" }.ExecuteNonQueryAsync();

Console.WriteLine("App finished. Waiting 15s to make sure all metrics are sent");
await Task.Delay(15000);
