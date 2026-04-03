using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using Ydb.Sdk.Ado;
using Ydb.Sdk.OpenTelemetry;

namespace Ydb.Sdk.AdoNet.OpenTelemetry.Metrics;

public static class LoadGenerator
{
    public static async Task Run()
    {
        const string serviceName = "ydb-load-generator";
        var otlpEndpoint = new Uri("http://otel-collector:4317");

        var resourceBuilder = ResourceBuilder.CreateDefault()
            .AddService(serviceName);

        using var meterProvider = global::OpenTelemetry.Sdk.CreateMeterProviderBuilder()
            .SetResourceBuilder(resourceBuilder)
            .AddYdb()
            .AddOtlpExporter((exporterOptions, metricReaderOptions) =>
            {
                exporterOptions.Endpoint = otlpEndpoint;
                metricReaderOptions.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds = 2000;
            })
            .Build();

        await using var dataSource = new YdbDataSource("Host=ydb;Port=2136;Database=/local");
 
        Console.WriteLine("=== YDB Metrics Load Generator Started ===");

        await using (var conn = await dataSource.OpenConnectionAsync())
        {
            try
            {
                await new YdbCommand("DROP TABLE load_test", conn).ExecuteNonQueryAsync();
            }
            catch
            {
                // ignored
            }

            await new YdbCommand("CREATE TABLE load_test(id Int32, val Int32, PRIMARY KEY (id))", conn)
                .ExecuteNonQueryAsync();
            await new YdbCommand("INSERT INTO load_test(id, val) VALUES (1, 0)", conn).ExecuteNonQueryAsync();
        }

        for (var step = 0; step < 1_000_000; step++)
        {
            Console.WriteLine($"[{DateTime.Now:T}] Step {step}: Sending requests...");

            var tasks = Enumerable.Range(0, 10).Select(_ => Task.Run(async () =>
            {
                try
                {
                    await using var conn = await dataSource.OpenConnectionAsync();
                    await new YdbCommand("UPDATE load_test SET val = val + 1 WHERE id = 1", conn)
                        .ExecuteNonQueryAsync();
                }
                catch
                {
                    // ignored
                }
            }));

            await Task.WhenAll(tasks);
        }
    }
}