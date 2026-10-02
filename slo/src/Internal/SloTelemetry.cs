using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;

namespace Internal;

public static class SloTelemetry
{
    public static MeterProvider? Create(SloConfig config, string job, string runId)
    {
        var endpoint = config.OtlpEndpoint
                       ?? Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT");
        if (string.IsNullOrWhiteSpace(endpoint))
        {
            var generic = Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT");
            if (string.IsNullOrWhiteSpace(generic)) return null;
            endpoint = $"{generic.TrimEnd('/')}/v1/metrics";
        }

        return Sdk.CreateMeterProviderBuilder()
            .AddMeter("Ydb.Sdk", "Ydb.Sdk.Topic")
            .ConfigureResource(resource => resource.AddService(job).AddAttributes(
            [
                new KeyValuePair<string, object>("ref", SloResult.Reference),
                new KeyValuePair<string, object>("workload",
                    Environment.GetEnvironmentVariable("WORKLOAD_NAME") ?? job),
                new KeyValuePair<string, object>("run_id", runId)
            ]))
            .AddOtlpExporter((options, readerOptions) =>
            {
                options.Protocol = OtlpExportProtocol.HttpProtobuf;
                options.Endpoint = new Uri(endpoint);
                readerOptions.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds =
                    config.ReportPeriod;
            })
            .Build();
    }
}