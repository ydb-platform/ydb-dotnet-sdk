using System.Diagnostics;
using System.Diagnostics.Metrics;
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

    public Task Create(CreateConfig createConfig);

    public Task Run(RunConfig runConfig);
}

public abstract class SloTableContext<T> : ISloContext
{
    private const int IntervalMs = 100;

    private static readonly ILogger Logger = ISloContext.Factory.CreateLogger<SloTableContext<T>>();

    private volatile int _maxId;

    protected abstract string Job { get; }

    protected abstract T CreateClient(Config config);

    public async Task Create(CreateConfig createConfig)
    {
        const int maxCreateAttempts = 10;
        var client = CreateClient(createConfig);

        for (var attempt = 0; attempt < maxCreateAttempts; attempt++)
        {
            Logger.LogInformation("Creating table {Name}...", SloTable.Name);
            try
            {
                await Create(client, createConfig.WriteTimeout);

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

        var tasks = new Task[createConfig.InitialDataCount];
        for (var i = 0; i < createConfig.InitialDataCount; i++)
        {
            tasks[i] = Save(client, createConfig);
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

    public async Task Run(RunConfig runConfig)
    {
        var refLabel = Environment.GetEnvironmentVariable("REF") ?? "unknown";
        var workloadLabel = Environment.GetEnvironmentVariable("WORKLOAD") ?? Job;
        
        var meterProvider = Sdk.CreateMeterProviderBuilder()
            .ConfigureResource(resource => resource
                .AddService(serviceName: $"workload-{workloadLabel}")
                .AddAttributes(new[]
                {
                    new KeyValuePair<string, object>("ref", refLabel),
                    new KeyValuePair<string, object>("sdk", "dotnet"),
                    new KeyValuePair<string, object>("sdk_version", Environment.Version.ToString())
                }))
            .AddMeter("YDB.SLO")
            .AddOtlpExporter((exporterOptions, metricReaderOptions) =>
            {
                // Prometheus OTLP endpoint: http://prometheus:9090/api/v1/otlp/v1/metrics
                var uri = new Uri(runConfig.OtlpEndpoint);
                var endpoint = $"{uri.Scheme}://{uri.Host}:{uri.Port}";
                var path = uri.AbsolutePath.TrimEnd('/');
                if (string.IsNullOrEmpty(path))
                {
                    path = "/api/v1/otlp/v1/metrics";
                }
                
                exporterOptions.Endpoint = new Uri($"{endpoint}{path}");
                exporterOptions.Protocol = OtlpExportProtocol.HttpProtobuf;
                
                metricReaderOptions.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds = runConfig.ReportPeriod;
            })
            .Build();

        var client = CreateClient(runConfig);

        _maxId = await SelectCount(client) + 1;

        Logger.LogInformation("Init row count: {MaxId}", _maxId);

        var writeLimiter = new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
        {
            Window = TimeSpan.FromMilliseconds(IntervalMs), PermitLimit = runConfig.WriteRps / 10,
            QueueLimit = int.MaxValue
        });
        var readLimiter = new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
        {
            Window = TimeSpan.FromMilliseconds(IntervalMs), PermitLimit = runConfig.ReadRps / 10,
            QueueLimit = int.MaxValue
        });

        var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(runConfig.Time));

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

        meterProvider?.Dispose();

        Logger.LogInformation("Run task is finished");
        return;

        async Task ShootingTask(RateLimiter rateLimitPolicy, string operationType, Func<T, RunConfig, Task> action)
        {
            var meter = new Meter("YDB.SLO");
            
            var tags = new TagList
            {
                { "operation_type", operationType },
                { "sdk", "dotnet" },
                { "sdk_version", Environment.Version.ToString() },
                { "workload", workloadLabel }
            };

            var operationsTotal = meter.CreateCounter<long>(
                "sdk.operations.total",
                description: "Total number of operations performed by the SDK, categorized by type."
            );

            var operationsSuccessTotal = meter.CreateCounter<long>(
                "sdk.operations.success.total",
                description: "Total number of successful operations, categorized by type."
            );

            var operationLatencySeconds = meter.CreateHistogram<double>(
                "sdk.operation.latency.seconds",
                unit: "s",
                description: "Latency of operations performed by the SDK in seconds, categorized by type and status."
            );

            var pendingOperations = meter.CreateUpDownCounter<long>(
                "sdk.pending.operations",
                description: "Current number of pending operations, categorized by type."
            );

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

                        pendingOperations.Add(1, tags);
                        var sw = Stopwatch.StartNew();
                        
                        try
                        {
                            await action(client, runConfig);
                            sw.Stop();
                            operationsTotal.Add(1, tags);
                            operationsSuccessTotal.Add(1, tags);
                            
                            var successTags = new TagList(tags) { { "operation_status", "success" } };
                            operationLatencySeconds.Record(sw.Elapsed.TotalSeconds, successTags);
                        }
                        catch (Exception ex)
                        {
                            sw.Stop();
                            operationsTotal.Add(1, tags);
                            
                            var failureTags = new TagList(tags) { { "operation_status", "failure" } };
                            operationLatencySeconds.Record(sw.Elapsed.TotalSeconds, failureTags);
                            
                            Logger.LogWarning(ex, "Operation {OperationType} failed", operationType);
                        }
                        finally
                        {
                            pendingOperations.Add(-1, tags);
                        }
                    }
                }, cancellationTokenSource.Token));
            }

            await Task.WhenAll(workJobs);

            Logger.LogInformation("{ShootingName} shooting is stopped", operationType);
        }
    }

    protected abstract Task<int> Save(T client, SloTable sloTable, int writeTimeout);

    protected abstract Task<object?> Select(T client, (Guid Guid, int Id) select, int readTimeout);

    protected abstract Task<int> SelectCount(T client);

    private Task<int> Save(T client, Config config)
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

    private async Task Select(T client, RunConfig config)
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
