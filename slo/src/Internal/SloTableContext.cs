using System.Runtime.ExceptionServices;
using System.Security.Cryptography;
using System.Threading.RateLimiting;
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
    private SloTable[] _controlRows = [];
    private readonly byte[] _keyNamespace = Guid.NewGuid().ToByteArray();

    protected abstract string Job { get; }

    protected abstract T CreateClient(SloConfig config);

    public async Task Create(SloConfig sloConfig)
    {
        if (sloConfig.ReadRps > 0 && sloConfig.InitialDataCount < 1)
        {
            throw new ArgumentException("KV reads require at least one prefilled control row");
        }

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

        var rows = new SloTable[sloConfig.InitialDataCount];
        var tasks = new Task[sloConfig.InitialDataCount];
        for (var i = 0; i < sloConfig.InitialDataCount; i++)
        {
            rows[i] = CreateRow(i + 1);
            tasks[i] = Save(client, rows[i], sloConfig.WriteTimeout);
        }

        try
        {
            await Task.WhenAll(tasks);
            _controlRows = rows;
            _maxId = rows.Length;
        }
        catch (Exception e)
        {
            Logger.LogError(e, "Initial data write failed");
            throw;
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
        var runId = Guid.NewGuid().ToString("N");

        await Create(sloConfig);

        using var meterProvider = Sdk.CreateMeterProviderBuilder()
            .ConfigureResource(resource => resource
                .AddService(serviceName: $"workload-{workloadLabel}")
                .AddAttributes([
                    new KeyValuePair<string, object>("ref", refLabel),
                    new KeyValuePair<string, object>("run_id", runId),
                    new KeyValuePair<string, object>("sdk", "dotnet"),
                    new KeyValuePair<string, object>("sdk_version", Environment.Version.ToString())
                ]))
            .AddMeter("Ydb.Sdk")
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

        var client = CreateClient(sloConfig);

        Logger.LogInformation("Init row count: {MaxId}", _maxId);

        using var writeLimiter = new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
        {
            Window = TimeSpan.FromMilliseconds(IntervalMs),
            PermitLimit = sloConfig.WriteRps / 10,
            QueueLimit = int.MaxValue
        });
        using var readLimiter = new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
        {
            Window = TimeSpan.FromMilliseconds(IntervalMs),
            PermitLimit = sloConfig.ReadRps / 10,
            QueueLimit = int.MaxValue
        });

        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(sloConfig.Time));
        ExceptionDispatchInfo? firstFailure = null;

        var writeTask = ShootingTask(writeLimiter, "write", Save, cancellationTokenSource);
        var readTask = ShootingTask(readLimiter, "read", Select, cancellationTokenSource);

        Logger.LogInformation("Started write / read shooting...");

        try
        {
            await Task.WhenAll(readTask, writeTask);
        }
        catch (OperationCanceledException e) when (
            firstFailure == null && cancellationTokenSource.IsCancellationRequested &&
            e.CancellationToken == cancellationTokenSource.Token)
        {
            Logger.LogInformation("Workload duration elapsed");
        }
        catch (Exception e)
        {
            Logger.LogError(e, "Workload failed");
            firstFailure?.Throw();
            throw;
        }

        Logger.LogInformation("Run task is finished");
        return;

        async Task ShootingTask(RateLimiter rateLimitPolicy, string operationType, Func<T, SloConfig, Task> action,
            CancellationTokenSource cancellation)
        {
            var workJobs = new List<Task>();

            for (var i = 0; i < 10; i++)
            {
                workJobs.Add(Task.Run(async () =>
                {
                    try
                    {
                        while (!cancellation.Token.IsCancellationRequested)
                        {
                            using var lease = await rateLimitPolicy
                                .AcquireAsync(cancellationToken: cancellation.Token);

                            if (!lease.IsAcquired)
                            {
                                await Task.Delay(Random.Shared.Next(IntervalMs / 2), cancellation.Token);
                            }

                            await action(client, sloConfig);
                        }
                    }
                    catch (OperationCanceledException e) when (
                        cancellation.IsCancellationRequested &&
                        e.CancellationToken == cancellation.Token)
                    {
                    }
                    catch (Exception ex)
                    {
                        Interlocked.CompareExchange(ref firstFailure, ExceptionDispatchInfo.Capture(ex), null);
                        Logger.LogError(ex, "Operation {OperationType} failed", operationType);
                        await cancellation.CancelAsync();
                        throw;
                    }
                }, cancellation.Token));
            }

            await Task.WhenAll(workJobs);

            Logger.LogInformation("{ShootingName} shooting is stopped", operationType);
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

    protected abstract Task<int> Save(T client, SloTable sloTable, int writeTimeout);

    protected abstract Task<SloTable?> Select(T client, (Guid Guid, int Id) select, int readTimeout);

    private SloTable CreateRow(int id)
    {
        const int minSizeStr = 20;
        const int maxSizeStr = 40;

        var timestamp = DateTime.UtcNow;
        return new SloTable
        {
            Guid = GuidFromInt(id),
            Id = id,
            PayloadStr = "v1:" + string.Join(string.Empty, Enumerable
                .Repeat(0, Random.Shared.Next(minSizeStr, maxSizeStr))
                .Select(_ => (char)Random.Shared.Next(127))),
            PayloadDouble = Random.Shared.NextDouble(),
            PayloadTimestamp = timestamp.AddTicks(-(timestamp.Ticks % 10))
        };
    }

    private Task<int> Save(T client, SloConfig config)
    {
        var row = CreateRow(Interlocked.Increment(ref _maxId));
        return Save(client, row, config.WriteTimeout);
    }

    private async Task Select(T client, SloConfig config)
    {
        // Load writes append after prefill; only these immutable owned rows are read.
        var expected = _controlRows[Random.Shared.Next(_controlRows.Length)];
        var actual = await Select(client, (expected.Guid, expected.Id), config.ReadTimeout);
        if (actual == null || actual.Guid != expected.Guid || actual.Id != expected.Id ||
            actual.PayloadStr != expected.PayloadStr ||
            BitConverter.DoubleToInt64Bits(actual.PayloadDouble) !=
            BitConverter.DoubleToInt64Bits(expected.PayloadDouble) ||
            actual.PayloadTimestamp != expected.PayloadTimestamp)
        {
            throw new InvalidOperationException($"KV data mismatch for control key {expected.Id}");
        }
    }

    private Guid GuidFromInt(int value)
    {
        var input = new byte[20];
        _keyNamespace.CopyTo(input, 0);
        BitConverter.GetBytes(value).CopyTo(input, 16);
        var hash = SHA1.HashData(input);
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
