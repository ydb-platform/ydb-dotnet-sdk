using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Security.Cryptography;
using System.Threading.RateLimiting;
using HdrHistogram;
using Microsoft.Extensions.Logging;
using NLog.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using Ydb.Sdk;

namespace Internal;

public interface ISloContext
{
    public static readonly ILoggerFactory Factory = LoggerFactory.Create(builder => builder.AddNLog());

    public Task Run(SloConfig sloConfig);
}

public abstract class SloTableContext<T> : ISloContext
{
    private const int IntervalMs = 100;

    private static readonly ILogger Logger = ISloContext.Factory.CreateLogger<SloTableContext<T>>();

    private volatile int _maxId;

    protected abstract string Job { get; }

    protected abstract T CreateClient(SloConfig config);

    public async Task Create(SloConfig sloConfig)
    {
        // Retries cover SCHEME_ERROR: schema cache on query nodes propagates async from
        // SchemeBoard after CREATE TABLE returns — a follow-up ALTER/INSERT can hit a node
        // whose cache still misses the table (ydb-platform/ydb#23386, #36335).
        const int maxCreateAttempts = 10;
        var client = CreateClient(sloConfig);

        for (var attempt = 0; attempt < maxCreateAttempts; attempt++)
        {
            Logger.LogInformation("Creating table {Name}...", SloTable.Name);
            try
            {
                await Create(client, sloConfig.WriteTimeout);

                Logger.LogInformation("Created table {Name}", SloTable.Name);

                break;
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Fail created table");

                if (attempt == maxCreateAttempts - 1)
                {
                    throw;
                }

                await Task.Delay(TimeSpan.FromSeconds(attempt));
            }
        }

        var tasks = new Task[sloConfig.InitialDataCount];
        for (var i = 0; i < sloConfig.InitialDataCount; i++)
        {
            tasks[i] = Save(client, sloConfig);
        }

        try
        {
            await Task.WhenAll(tasks);
        }
        catch (Exception e)
        {
            Logger.LogError(e, "Init failed when all tasks, continue..");
        }
        finally
        {
            Logger.LogInformation("Created task is finished");
        }
    }

    protected abstract Task Create(T client, int operationTimeout);

    public async Task Run(SloConfig sloConfig)
    {
        var refLabel = Environment.GetEnvironmentVariable("WORKLOAD_REF") ?? "unknown";
        var workloadLabel = Environment.GetEnvironmentVariable("WORKLOAD_NAME") ?? Job;

        await Create(sloConfig);

        using var meter = new Meter("YDB.SLO");

        var operationsTotal = meter.CreateCounter<long>(
            "sdk.operations.total",
            description: "Total number of operations, by type and status."
        );

        // Latency is measured only for successful operations (matches go-sdk / js-sdk SLO contract);
        // failed operations are reflected via sdk.operations.total{operation_status="error"} only.
        var latencyAggregators = new Dictionary<string, LatencyAggregator>
        {
            ["read"] = new(),
            ["write"] = new()
        };

        // Snapshots refreshed on each push tick. ObservableGauge callbacks read from here.
        var snapshots = latencyAggregators.Keys
            .ToDictionary(k => k, _ => (P50: 0.0, P95: 0.0, P99: 0.0));
        var snapshotLock = new object();

        meter.CreateObservableGauge(
            "sdk.operation.latency.p50.seconds",
            () => SnapshotMeasurements(snapshots, snapshotLock, refLabel, s => s.P50),
            unit: "s",
            description: "P50 latency of operations, recomputed each push period.");
        meter.CreateObservableGauge(
            "sdk.operation.latency.p95.seconds",
            () => SnapshotMeasurements(snapshots, snapshotLock, refLabel, s => s.P95),
            unit: "s",
            description: "P95 latency of operations, recomputed each push period.");
        meter.CreateObservableGauge(
            "sdk.operation.latency.p99.seconds",
            () => SnapshotMeasurements(snapshots, snapshotLock, refLabel, s => s.P99),
            unit: "s",
            description: "P99 latency of operations, recomputed each push period.");

        var meterProvider = Sdk.CreateMeterProviderBuilder()
            .ConfigureResource(resource => resource
                .AddService(serviceName: $"workload-{workloadLabel}")
                .AddAttributes([
                    new KeyValuePair<string, object>("sdk", "dotnet"),
                    new KeyValuePair<string, object>("sdk_version", Environment.Version.ToString())
                ]))
            .AddMeter("YDB.SLO")
            .AddOtlpExporter((exporterOptions, metricReaderOptions) =>
            {
                var endpointUri = ResolveOtlpEndpoint(sloConfig.OtlpEndpoint);
                if (endpointUri != null)
                {
                    exporterOptions.Endpoint = endpointUri;
                }

                exporterOptions.Protocol = OtlpExportProtocol.HttpProtobuf;
                metricReaderOptions.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds =
                    sloConfig.ReportPeriod;
            })
            .Build();

        // Refresh percentile snapshots and reset histograms one push period before export so
        // the gauge callbacks observe fresh values when the SDK harvests them.
        using var snapshotTimer = new Timer(_ =>
        {
            foreach (var kv in latencyAggregators)
            {
                var snapshot = kv.Value.SnapshotAndReset();
                lock (snapshotLock)
                {
                    snapshots[kv.Key] = snapshot;
                }
            }
        }, null, sloConfig.ReportPeriod, sloConfig.ReportPeriod);

        var client = CreateClient(sloConfig);

        _maxId = await SelectCount(client) + 1;

        Logger.LogInformation("Init row count: {MaxId}", _maxId);

        var writeLimiter = new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
        {
            Window = TimeSpan.FromMilliseconds(IntervalMs), PermitLimit = sloConfig.WriteRps / 10,
            QueueLimit = int.MaxValue
        });
        var readLimiter = new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
        {
            Window = TimeSpan.FromMilliseconds(IntervalMs), PermitLimit = sloConfig.ReadRps / 10,
            QueueLimit = int.MaxValue
        });

        var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(sloConfig.Time));

        var writeTask = ShootingTask(writeLimiter, "write", Save);
        var readTask = ShootingTask(readLimiter, "read", Select);

        Logger.LogInformation("Started write / read shooting...");

        try
        {
            await Task.WhenAll(readTask, writeTask);
        }
        catch (Exception e)
        {
            Logger.LogInformation(e, "Cancel shooting");
        }

        meterProvider.Dispose();

        Logger.LogInformation("Run task is finished");
        return;

        async Task ShootingTask(RateLimiter rateLimitPolicy, string operationType, Func<T, SloConfig, Task> action)
        {
            var successTags = new TagList
            {
                { "operation_type", operationType },
                { "operation_status", "success" },
                { "ref", refLabel }
            };
            var errorTags = new TagList
            {
                { "operation_type", operationType },
                { "operation_status", "error" },
                { "ref", refLabel }
            };

            var workJobs = new List<Task>();

            for (var i = 0; i < 10; i++)
            {
                workJobs.Add(Task.Run(async () =>
                {
                    while (!cancellationTokenSource.Token.IsCancellationRequested)
                    {
                        using var lease = await rateLimitPolicy
                            .AcquireAsync(cancellationToken: cancellationTokenSource.Token);

                        if (!lease.IsAcquired)
                        {
                            await Task.Delay(Random.Shared.Next(IntervalMs / 2), cancellationTokenSource.Token);
                        }

                        var sw = Stopwatch.StartNew();

                        try
                        {
                            await action(client, sloConfig);
                            sw.Stop();
                            operationsTotal.Add(1, successTags);
                            latencyAggregators[operationType].Record(sw.Elapsed.TotalSeconds);
                        }
                        catch (Exception ex)
                        {
                            operationsTotal.Add(1, errorTags);
                            Logger.LogWarning(ex, "Operation {OperationType} failed", operationType);
                        }
                    }
                }, cancellationTokenSource.Token));
            }

            await Task.WhenAll(workJobs);

            Logger.LogInformation("{ShootingName} shooting is stopped", operationType);
        }
    }

    private static IEnumerable<Measurement<double>> SnapshotMeasurements(
        Dictionary<string, (double P50, double P95, double P99)> snapshots,
        object snapshotLock,
        string refLabel,
        Func<(double P50, double P95, double P99), double> selector)
    {
        lock (snapshotLock)
        {
            return snapshots
                .Select(kv => new Measurement<double>(
                    selector(kv.Value),
                    new KeyValuePair<string, object?>("operation_type", kv.Key),
                    new KeyValuePair<string, object?>("operation_status", "success"),
                    new KeyValuePair<string, object?>("ref", refLabel)))
                .ToArray();
        }
    }

    private static Uri? ResolveOtlpEndpoint(string? cliEndpoint)
    {
        // Priority: CLI flag > OTEL_EXPORTER_OTLP_METRICS_ENDPOINT > OTEL_EXPORTER_OTLP_ENDPOINT.
        // When falling back to the generic OTEL_EXPORTER_OTLP_ENDPOINT, append the Prometheus
        // OTLP metrics path so the exporter targets the metrics receiver directly.
        var raw = !string.IsNullOrWhiteSpace(cliEndpoint)
            ? cliEndpoint
            : Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT");

        if (!string.IsNullOrWhiteSpace(raw))
        {
            return new Uri(raw);
        }

        var generic = Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT");
        if (string.IsNullOrWhiteSpace(generic))
        {
            return null;
        }

        var trimmed = generic.TrimEnd('/');
        return new Uri($"{trimmed}/v1/metrics");
    }

    private sealed class LatencyAggregator
    {
        private const long HighestTrackableMicros = 60L * 60L * 1_000_000L; // 1 hour
        private const int SignificantDigits = 3;

        private readonly LongHistogram _histogram = new(HighestTrackableMicros, SignificantDigits);
        private readonly object _lock = new();

        public void Record(double seconds)
        {
            var micros = (long)(seconds * 1_000_000d);
            if (micros < 1) micros = 1;
            if (micros > HighestTrackableMicros) micros = HighestTrackableMicros;

            lock (_lock)
            {
                _histogram.RecordValue(micros);
            }
        }

        public (double P50, double P95, double P99) SnapshotAndReset()
        {
            lock (_lock)
            {
                if (_histogram.TotalCount == 0)
                {
                    return (0d, 0d, 0d);
                }

                var p50 = _histogram.GetValueAtPercentile(50) / 1_000_000d;
                var p95 = _histogram.GetValueAtPercentile(95) / 1_000_000d;
                var p99 = _histogram.GetValueAtPercentile(99) / 1_000_000d;
                _histogram.Reset();
                return (p50, p95, p99);
            }
        }
    }

    protected abstract Task<int> Save(T client, SloTable sloTable, int writeTimeout);

    protected abstract Task<object?> Select(T client, (Guid Guid, int Id) select, int readTimeout);

    protected abstract Task<int> SelectCount(T client);

    private Task<int> Save(T client, SloConfig config)
    {
        const int minSizeStr = 20;
        const int maxSizeStr = 40;

        var id = Interlocked.Increment(ref _maxId);
        var sloTable = new SloTable
        {
            Guid = GuidFromInt(id),
            Id = id,
            PayloadStr = string.Join(string.Empty, Enumerable
                .Repeat(0, Random.Shared.Next(minSizeStr, maxSizeStr))
                .Select(_ => (char)Random.Shared.Next(127))),
            PayloadDouble = Random.Shared.NextDouble(),
            PayloadTimestamp = DateTime.Now
        };

        return Save(client, sloTable, config.WriteTimeout);
    }

    private async Task Select(T client, SloConfig config)
    {
        var id = Random.Shared.Next(_maxId);
        _ = await Select(client, new ValueTuple<Guid, int>(GuidFromInt(id), id), config.ReadTimeout);
    }

    private static Guid GuidFromInt(int value)
    {
        var intBytes = BitConverter.GetBytes(value);
        var hash = SHA1.HashData(intBytes);
        var guidBytes = new byte[16];
        Array.Copy(hash, guidBytes, 16);
        return new Guid(guidBytes);
    }
}

public static class StatusCodeExtension
{
    public static string StatusName(this StatusCode statusCode)
    {
        var prefix = statusCode >= StatusCode.ClientTransportResourceExhausted ? "GRPC" : "YDB";
        return $"{prefix}_{statusCode}";
    }
}