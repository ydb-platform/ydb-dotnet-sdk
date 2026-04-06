using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using Ydb.Sdk.Ado;
using Ydb.Sdk.OpenTelemetry;

await using var dataSource = new YdbDataSource("Host=ydb;Port=2136;Database=/local");

if (args.Length > 0 && args[0] == "--load")
{
    var durationSeconds = args.Length > 1 && int.TryParse(args[1], out var s) ? s : 600;
    using var loadMeterProvider = Sdk.CreateMeterProviderBuilder()
        .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("ydb-load-generator"))
        .AddYdb()
        .AddOtlpExporter((exporterOptions, metricReaderOptions) =>
        {
            exporterOptions.Endpoint = new Uri("http://otel-collector:4317");
            metricReaderOptions.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds = 2000;
        })
        .Build();

    await dataSource.ExecuteAsync(async conn =>
    {
        await new YdbCommand("DROP TABLE IF EXISTS load_test", conn).ExecuteNonQueryAsync();
        await new YdbCommand("CREATE TABLE load_test(id Uuid, name Text, PRIMARY KEY (id))", conn)
            .ExecuteNonQueryAsync();
    });

    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(durationSeconds));
    Console.WriteLine($"=== YDB Metrics Load Generator Started ({durationSeconds}s) ===");

    var workers = Enumerable.Range(0, 10).Select(_ => Task.Run(async () =>
    {
        while (!cts.IsCancellationRequested)
        {
            try
            {
                await dataSource.ExecuteAsync(async conn =>
                    await new YdbCommand("INSERT INTO load_test(id, name) VALUES (RandomUuid(0), 'Text')", conn)
                        .ExecuteNonQueryAsync());
            }
            catch
            {
                /* ignored */
            }
        }
    }));

    await Task.WhenAll(workers);
    return;
}

const string serviceName = "ydb-sdk-otel-metrics-sample";
var otlpEndpoint = new Uri("http://otel-collector:4317");

var serviceVersion = typeof(Program).Assembly.GetName().Version?.ToString() ?? "unknown";

var resourceBuilder = ResourceBuilder.CreateDefault()
    .AddService(serviceName, serviceVersion: serviceVersion);

using var meterProvider = Sdk.CreateMeterProviderBuilder()
    .SetResourceBuilder(resourceBuilder)
    .AddRuntimeInstrumentation()
    .AddMeter()
    .AddOtlpExporter(o => { o.Endpoint = otlpEndpoint; })
    .Build();

Console.WriteLine($"[{DateTimeOffset.UtcNow:u}] started, service.name={serviceName}");

Console.WriteLine("Initializing...");

await using var connInit = await dataSource.OpenConnectionAsync();
await new YdbCommand("DROP TABLE IF EXISTS bank", connInit).ExecuteNonQueryAsync();
await new YdbCommand("CREATE TABLE bank(id Int32, amount Int32, PRIMARY KEY (id))", connInit)
    .ExecuteNonQueryAsync();

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
