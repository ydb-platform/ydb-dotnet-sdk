using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Reflection;
using System.Runtime.InteropServices;
using HdrHistogram;
using Internal;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.RetryPolicy;

namespace Rmw;

public sealed class RmwContext : ISloContext
{
    public async Task Run(SloConfig config)
    {
        const int writers = 4;
        const int counters = 2;
        if (config.Time < 1 || config.ReportPeriod < 1)
            throw new ArgumentOutOfRangeException(nameof(config.Time));

        await using var client = new YdbDataSourceBuilder(
                new YdbConnectionStringBuilder(config.ConnectionString) { LoggerFactory = ISloContext.Factory })
            { RetryPolicy = YdbRetryPolicy.IdempotenceDefault }.Build();
        using var metrics = new RmwMetrics(config);
        using var store = new AdoRmwStore(client, config.WriteTimeout, config.ReadTimeout, metrics.RecordAttempts);
        using var stop = new CancellationTokenSource();
        var shutdown = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var term = PosixSignalRegistration.Create(PosixSignal.SIGTERM, context =>
        {
            context.Cancel = true;
            shutdown.TrySetResult();
        });
        using var interrupt = PosixSignalRegistration.Create(PosixSignal.SIGINT, context =>
        {
            context.Cancel = true;
            shutdown.TrySetResult();
        });
        Console.WriteLine($"RMW namespace={store.Namespace} writers={writers} counters={counters}");
        try
        {
            await store.Create(counters);
            stop.CancelAfter(TimeSpan.FromSeconds(config.Time));
            if (shutdown.Task.IsCompleted) stop.Cancel();
            var loading = RmwScenario.Run(store, writers, counters, stop.Token, record: metrics.Record);
            await Task.WhenAny(loading, shutdown.Task);
            if (shutdown.Task.IsCompleted) stop.Cancel();
            var count = await loading;
            Console.WriteLine($"RMW verified namespace={store.Namespace} unique_operations={count}");
            if (count == 0 && !stop.IsCancellationRequested)
                throw new InvalidOperationException("RMW completed without operations");
        }
        finally
        {
            try
            {
                await store.Drop();
            }
            catch (Exception error)
            {
                // Cleanup is diagnostic; it must not replace an original terminal transaction failure.
                Console.Error.WriteLine($"RMW cleanup failed for {store.Namespace}: {error}");
            }
        }
    }

    private sealed class RmwMetrics : IDisposable
    {
        private readonly Meter _meter = new("YDB.SLO");
        private readonly Counter<long> _operations;
        private readonly Counter<long> _attempts;
        private readonly MeterProvider _provider;
        private readonly LongHistogram _histogram = new(60L * 60 * 1_000_000, 3);
        private readonly object _lock = new();
        private readonly Timer _timer;
        private readonly string _ref = Environment.GetEnvironmentVariable("WORKLOAD_REF") ?? "unknown";
        private double _p50, _p95, _p99;

        public RmwMetrics(SloConfig config)
        {
            _operations = _meter.CreateCounter<long>("sdk.operations.total");
            _attempts = _meter.CreateCounter<long>("sdk.retry.attempts.total");
            Gauge("sdk.operation.latency.p50.seconds", () => _p50);
            Gauge("sdk.operation.latency.p95.seconds", () => _p95);
            Gauge("sdk.operation.latency.p99.seconds", () => _p99);
            _provider = Sdk.CreateMeterProviderBuilder()
                .ConfigureResource(resource => resource
                    .AddService($"workload-{Environment.GetEnvironmentVariable("WORKLOAD_NAME") ?? "Rmw"}")
                    .AddAttributes([
                        new KeyValuePair<string, object>("sdk", "dotnet"),
                        new KeyValuePair<string, object>("sdk_version", typeof(YdbConnection).Assembly
                                                                            .GetCustomAttribute<
                                                                                AssemblyInformationalVersionAttribute>()
                                                                            ?.InformationalVersion ??
                                                                        "unknown"),
                        new KeyValuePair<string, object>("ref", _ref)
                    ]))
                .AddMeter("YDB.SLO", "Ydb.Sdk")
                .AddOtlpExporter((exporter, reader) =>
                {
                    var endpoint = config.OtlpEndpoint ??
                                   Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT");
                    var generic = Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT");
                    if (!string.IsNullOrWhiteSpace(endpoint)) exporter.Endpoint = new Uri(endpoint);
                    else if (!string.IsNullOrWhiteSpace(generic))
                        exporter.Endpoint = new Uri($"{generic.TrimEnd('/')}/v1/metrics");
                    exporter.Protocol = OtlpExportProtocol.HttpProtobuf;
                    reader.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds = config.ReportPeriod;
                }).Build();
            _timer = new Timer(_ =>
            {
                lock (_lock)
                {
                    if (_histogram.TotalCount == 0)
                    {
                        _p50 = _p95 = _p99 = 0;
                        return;
                    }

                    _p50 = _histogram.GetValueAtPercentile(50) / 1_000_000d;
                    _p95 = _histogram.GetValueAtPercentile(95) / 1_000_000d;
                    _p99 = _histogram.GetValueAtPercentile(99) / 1_000_000d;
                    _histogram.Reset();
                }
            }, null, config.ReportPeriod, config.ReportPeriod);
        }

        private void Gauge(string name, Func<double> value) => _meter.CreateObservableGauge(name, () =>
        {
            lock (_lock)
                return new Measurement<double>(value(),
                    new KeyValuePair<string, object?>("operation_type", "write"),
                    new KeyValuePair<string, object?>("operation_status", "success"),
                    new KeyValuePair<string, object?>("ref", _ref));
        }, unit: "s");

        public void RecordAttempts(int attempts) => _attempts.Add(attempts,
            new KeyValuePair<string, object?>("operation_type", "write"),
            new KeyValuePair<string, object?>("ref", _ref));

        public void Record(bool success, TimeSpan elapsed)
        {
            var tags = new TagList
            {
                { "operation_type", "write" }, { "operation_status", success ? "success" : "error" },
                { "ref", _ref }
            };
            _operations.Add(1, tags);
            if (success)
                lock (_lock)
                    _histogram.RecordValue(Math.Clamp((long)(elapsed.TotalSeconds * 1_000_000), 1, 3_600_000_000));
        }

        public void Dispose()
        {
            _timer.Dispose();
            _provider.Dispose();
            _meter.Dispose();
        }
    }
}
