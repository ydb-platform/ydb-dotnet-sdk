using System.Diagnostics;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

var serviceName = Environment.GetEnvironmentVariable("OTEL_SERVICE_NAME") ?? "ydb-sdk-adonet-sample";
var serviceVersion = typeof(Program).Assembly.GetName().Version?.ToString() ?? "unknown";

var resourceBuilder = ResourceBuilder.CreateDefault()
    .AddService(serviceName: serviceName, serviceVersion: serviceVersion);

var activitySourceName = "Ydb.Sdk.AdoNet.OpenTelemetry.Sample";
using var activitySource = new ActivitySource(activitySourceName);

using var tracerProvider = Sdk.CreateTracerProviderBuilder()
    .SetResourceBuilder(resourceBuilder)
    .AddSource(activitySourceName)
    .AddOtlpExporter()
    .Build();

using var meterProvider = Sdk.CreateMeterProviderBuilder()
    .SetResourceBuilder(resourceBuilder)
    .AddRuntimeInstrumentation()
    .AddOtlpExporter()
    .Build();

Console.WriteLine($"[{DateTimeOffset.UtcNow:u}] started, service.name={serviceName}");

using (var activity = activitySource.StartActivity("app.startup", ActivityKind.Internal))
{
    activity?.SetTag("app.message", "hello");
}

using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
while (await timer.WaitForNextTickAsync())
{
    using var tick = activitySource.StartActivity("app.tick", ActivityKind.Internal);
    tick?.SetTag("tick.utc", DateTimeOffset.UtcNow.ToString("u"));
    Console.WriteLine($"[{DateTimeOffset.UtcNow:u}] tick");
}