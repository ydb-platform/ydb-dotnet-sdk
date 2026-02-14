using System.Diagnostics;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Ydb.Sdk.Ado;

const string serviceName = "ydb-sdk-adonet-sample";
var otlpEndpoint = new Uri("http://otel-collector:4317");

var serviceVersion = typeof(Program).Assembly.GetName().Version?.ToString() ?? "unknown";

var resourceBuilder = ResourceBuilder.CreateDefault()
    .AddService(serviceName: serviceName, serviceVersion: serviceVersion);

const string activitySourceName = "Ydb.Sdk.AdoNet.OpenTelemetry.Sample";
using var activitySource = new ActivitySource(activitySourceName);

using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .SetResourceBuilder(resourceBuilder)
    .AddSource(activitySourceName)
    .AddSource("Ydb.Sdk")
    .AddOtlpExporter(o => { o.Endpoint = otlpEndpoint; })
    .Build();

using var meterProvider = Sdk.CreateMeterProviderBuilder()
    .SetResourceBuilder(resourceBuilder)
    .AddRuntimeInstrumentation()
    .AddOtlpExporter(o => { o.Endpoint = otlpEndpoint; })
    .Build();

Console.WriteLine($"[{DateTimeOffset.UtcNow:u}] started, service.name={serviceName}");

await using var dataSource = new YdbDataSource("Host=ydb;Port=2136;Database=/local");

const string appStartup = "app.startup";
using (var activity = activitySource.StartActivity(appStartup))
{
    activity?.SetTag("app.message", "hello");

    await using var conn = await dataSource.OpenConnectionAsync();
    await using var cmd = new YdbCommand("CREATE TABLE a(b Uuid, PRIMARY KEY (b))", conn);
    _ = await cmd.ExecuteScalarAsync();
}

using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
while (await timer.WaitForNextTickAsync())
{
    const string appTick = "app.tick";
    using var tick = activitySource.StartActivity(appTick, ActivityKind.Client);
    tick?.SetTag("tick.utc", DateTimeOffset.UtcNow.ToString("u"));
    Console.WriteLine($"[{DateTimeOffset.UtcNow:u}] tick");

    await using var conn = await dataSource.OpenConnectionAsync();
    await using var cmd = new YdbCommand("INSERT INTO a(b) VALUES (@b)", conn);
    cmd.Parameters.AddWithValue("b", Guid.NewGuid());
    _ = await cmd.ExecuteScalarAsync();
}