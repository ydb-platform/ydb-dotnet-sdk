using System.Diagnostics;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Ydb.Sdk.Ado;
using Ydb.Sdk.OpenTelemetry;

const string serviceName = "ydb-sdk-sample";
var otlpEndpoint = new Uri("http://otel-collector:4317");

var serviceVersion = typeof(Program).Assembly.GetName().Version?.ToString() ?? "unknown";

var resourceBuilder = ResourceBuilder.CreateDefault()
    .AddService(serviceName: serviceName, serviceVersion: serviceVersion);

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
    .AddOtlpExporter(o => { o.Endpoint = otlpEndpoint; })
    .Build();

Console.WriteLine($"[{DateTimeOffset.UtcNow:u}] started, service.name={serviceName}");

Console.WriteLine("Initializing...");

await using var dataSource = new YdbDataSource("Host=ydb;Port=2136;Database=/local");
const string appStartup = "app.startup";
using (var activity = activitySource.StartActivity(appStartup))
{
    activity?.SetTag("app.message", "hello");

    await using var connInit = await dataSource.OpenConnectionAsync();
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

// Console.WriteLine("Emulation TLI...");
//
var tasks = new List<Task>();
// for (var i = 0; i < 10; i++)
// {
//     var concurrentTaskNum = i;
//     tasks.Add(Task.Run(async () =>
//     {
//         const string exampleTli = "example_tli";
//         // ReSharper disable once AccessToDisposedClosure
//         using var concurrentActivity = activitySource.StartActivity(exampleTli);
//         concurrentActivity?.SetTag("app.message", $"concurrent task {concurrentTaskNum}");
//
//         // ReSharper disable once AccessToDisposedClosure
//         await dataSource.ExecuteInTransactionAsync(async ydbConnection =>
//         {
//             var count = (int)(await new YdbCommand(ydbConnection)
//                 { CommandText = "SELECT amount FROM bank WHERE id = 1" }.ExecuteScalarAsync())!;
//
//             await new YdbCommand(ydbConnection)
//             {
//                 CommandText = "UPDATE bank SET amount = @amount + 1 WHERE id = 1",
//                 Parameters = { new YdbParameter { Value = count, ParameterName = "amount" } }
//             }.ExecuteNonQueryAsync();
//         });
//     }));
// }
//
// await Task.WhenAll(tasks);
tasks.Clear();

Console.WriteLine("Emulation RetryConnection...");

for (var i = 0; i < 100; i++)
{
    var concurrentTaskNum = i;
    tasks.Add(Task.Run(async () =>
    {
        const string exampleTli = "retry_connection_tli";
        // ReSharper disable once AccessToDisposedClosure
        using var concurrentActivity = activitySource.StartActivity(exampleTli);
        concurrentActivity?.SetTag("app.message", $"concurrent task {concurrentTaskNum}");
        // ReSharper disable once AccessToDisposedClosure
        await using var ydbConnection = await dataSource.OpenRetryableConnectionAsync();

        await new YdbCommand(ydbConnection)
        {
            CommandText = "INSERT INTO bank(id, amount) VALUES (@id, 0)",
            Parameters = { new YdbParameter { Value = concurrentTaskNum, ParameterName = "id" } }
        }.ExecuteNonQueryAsync();
    }));
}

await Task.WhenAll(tasks);

Console.WriteLine("App finished.");