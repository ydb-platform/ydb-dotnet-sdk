using System.Diagnostics;
using System.Security.Cryptography;
using System.Threading.RateLimiting;
using Microsoft.Extensions.Logging;
using Prometheus;
using Ydb.Sdk;

namespace Internal;

public interface ISloContext
{
    // ReSharper disable once StaticMemberInGenericType
    public static readonly ILoggerFactory Factory =
        LoggerFactory.Create(builder =>
        {
            builder.AddConsole().SetMinimumLevel(LogLevel.Information);
            builder.AddFilter("Ydb.Sdk.Ado", LogLevel.Debug);
            builder.AddFilter("Ydb.Sdk.Services.Query", LogLevel.Debug);
        });


    public Task Create(CreateConfig config);

    public Task Run(RunConfig runConfig);
}

public abstract class SloTableContextBase : ISloContext
{
    protected static readonly ILogger Logger = ISloContext.Factory.CreateLogger<SloTableContextBase>();

    private volatile int _maxId;

    protected abstract string Job { get; }

    public async Task Create(CreateConfig config)
    {
        const int maxCreateAttempts = 10;

        for (var attempt = 0; attempt < maxCreateAttempts; attempt++)
        {
            Logger.LogInformation("Creating table {Name}...", SloTable.Name);
            try
            {
                await Create(config.WriteTimeout);

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

        var tasks = new Task[config.InitialDataCount];
        for (var i = 0; i < config.InitialDataCount; i++)
        {
            tasks[i] = Save(config);
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

    protected abstract Task Create(int operationTimeout);

    public async Task Run(RunConfig runConfig)
    {
        // Trace.Listeners.Add(new ConsoleTraceListener()); debug meterPusher

        var promPgwEndpoint = $"{runConfig.PromPgw}/metrics";
        using var prometheus = new MetricPusher(promPgwEndpoint, "workload-" + Job,
            intervalMilliseconds: runConfig.ReportPeriod);
        prometheus.Start();

        _maxId = await SelectCount() + 1;

        Logger.LogInformation("Init row count: {MaxId}", _maxId);

        var writeLimiter = new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
        {
            Window = TimeSpan.FromMilliseconds(100), PermitLimit = runConfig.WriteRps / 10, QueueLimit = int.MaxValue
        });
        var readLimiter = new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
        {
            Window = TimeSpan.FromMilliseconds(100), PermitLimit = runConfig.ReadRps / 10, QueueLimit = int.MaxValue
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

        await prometheus.StopAsync();

        Logger.LogInformation("Run task is finished");
        return;

        Task ShootingTask(RateLimiter rateLimitPolicy, string operationType,
            Func<RunConfig, Counter?, Task<(int, StatusCode)>> action)
        {
            var metricFactory = Metrics.WithLabels(new Dictionary<string, string>
                {
                    { "operation_type", operationType },
                    { "sdk", "dotnet" },
                    { "sdk_version", Environment.Version.ToString() },
                    { "workload", Job },
                    { "workload_version", "0.0.0" }
                }
            );

            var operationsTotal = metricFactory.CreateCounter(
                "sdk_operations_total",
                "Total number of operations performed by the SDK, categorized by type."
            );

            var operationsSuccessTotal = metricFactory.CreateCounter(
                "sdk_operations_success_total",
                "Total number of successful operations, categorized by type."
            );

            var operationsFailureTotal = metricFactory.CreateCounter(
                "sdk_operations_failure_total",
                "Total number of failed operations, categorized by type."
            );

            var operationLatencySeconds = metricFactory.CreateHistogram(
                "sdk_operation_latency_seconds",
                "Latency of operations performed by the SDK in seconds, categorized by type and status.",
                ["operation_status"],
                new HistogramConfiguration
                {
                    Buckets =
                    [
                        0.001, // 1 ms
                        0.002, // 2 ms
                        0.003, // 3 ms
                        0.004, // 4 ms
                        0.005, // 5 ms
                        0.0075, // 7.5 ms
                        0.010, // 10 ms
                        0.020, // 20 ms
                        0.050, // 50 ms
                        0.100, // 100 ms
                        0.200, // 200 ms
                        0.500, // 500 ms
                        1.000 // 1 s
                    ]
                }
            );

            var retryAttempts = metricFactory.CreateGauge(
                "sdk_retry_attempts",
                "Current retry attempts, categorized by operation type."
            );

            var pendingOperations = metricFactory.CreateGauge(
                "sdk_pending_operations",
                "Current number of pending operations, categorized by type."
            );

            var errorsTotal = metricFactory.CreateCounter(
                "sdk_errors_total",
                "Total number of errors encountered, categorized by error type.",
                ["error_type"]
            );

            // ReSharper disable once MethodSupportsCancellation
            return Task.Run(async () =>
            {
                while (!cancellationTokenSource.Token.IsCancellationRequested)
                {
                    using var lease = await rateLimitPolicy
                        .AcquireAsync(cancellationToken: cancellationTokenSource.Token);

                    if (!lease.IsAcquired)
                    {
                        continue;
                    }

                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            pendingOperations.Inc();
                            var sw = Stopwatch.StartNew();
                            var (attempts, statusCode) = await action(runConfig, errorsTotal);
                            sw.Stop();

                            retryAttempts.Set(attempts);
                            operationsTotal.Inc();
                            pendingOperations.Dec();

                            if (statusCode != StatusCode.Success)
                            {
                                errorsTotal.WithLabels(statusCode.StatusName()).Inc();
                                operationsFailureTotal.Inc();
                                operationLatencySeconds.WithLabels("err").Observe(sw.Elapsed.TotalSeconds);
                            }
                            else
                            {
                                operationsSuccessTotal.Inc();
                                operationLatencySeconds.WithLabels("success").Observe(sw.Elapsed.TotalSeconds);
                            }
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine(e);
                        }
                    }, cancellationTokenSource.Token);
                }

                Logger.LogInformation("{ShootingName} shooting is stopped", operationType);
            });
        }
    }

    // return attempt count & StatusCode operation
    protected abstract Task<(int, StatusCode)> Save(SloTable sloTable, int writeTimeout,
        Counter? errorsTotal = null);

    protected abstract Task<(int, StatusCode, object?)> Select((Guid Guid, int Id) select, int readTimeout,
        Counter? errorsTotal = null);

    protected abstract Task<int> SelectCount();

    private Task<(int, StatusCode)> Save(Config config, Counter? errorsTotal = null)
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

        return Save(sloTable, config.WriteTimeout, errorsTotal);
    }

    private async Task<(int, StatusCode)> Select(RunConfig config, Counter? errorsTotal = null)
    {
        var id = Random.Shared.Next(_maxId);
        var (attempts, code, _) =
            await Select(new ValueTuple<Guid, int>(GuidFromInt(id), id), config.ReadTimeout, errorsTotal);

        return (attempts, code);
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