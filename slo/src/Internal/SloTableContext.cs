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

        var meterProvider = Sdk.CreateMeterProviderBuilder()
            .ConfigureResource(resource => resource
                .AddService(serviceName: $"workload-{workloadLabel}")
                .AddAttributes([
                    new KeyValuePair<string, object>("ref", refLabel),
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

                        try
                        {
                            await action(client, sloConfig);
                        }
                        catch (Exception ex)
                        {
                            Logger.LogWarning(ex, "Operation {OperationType} failed", operationType);
                        }
                    }
                }, cancellationTokenSource.Token));
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