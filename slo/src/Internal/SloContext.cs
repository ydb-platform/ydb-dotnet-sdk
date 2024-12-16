using System.Diagnostics;
using System.Threading.RateLimiting;
using Microsoft.Extensions.Logging;
using Prometheus;
using Ydb.Sdk;
using Ydb.Sdk.Value;

namespace Internal;

public abstract class SloContext<T> where T : IDisposable
{
    // ReSharper disable once StaticMemberInGenericType
    protected static readonly ILoggerFactory Factory =
        LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Debug));

    protected static readonly ILogger Logger = Factory.CreateLogger<SloContext<T>>();

    private volatile int _maxId;

    protected abstract string Job { get; }

    public async Task Create(CreateConfig config)
    {
        const int maxCreateAttempts = 10;

        using var client = await CreateClient(config);
        for (var attempt = 0; attempt < maxCreateAttempts; attempt++)
        {
            Logger.LogInformation("Creating table {ResourcePathYdb}..", config.ResourcePathYdb);
            try
            {
                var createTableSql = $"""
                                      CREATE TABLE `{config.ResourcePathYdb}` (
                                          hash              Uint64,
                                          id                Int32,
                                          payload_str       Text,
                                          payload_double    Double,
                                          payload_timestamp Timestamp,
                                          payload_hash      Uint64,
                                          PRIMARY KEY (hash, id)
                                      ) WITH (
                                          AUTO_PARTITIONING_BY_SIZE = ENABLED,
                                          AUTO_PARTITIONING_BY_LOAD = ENABLED,
                                          AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {config.MinPartitionsCount},
                                          AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = {config.MaxPartitionsCount}
                                      );
                                      """;
                Logger.LogInformation("YQL script: {sql}", createTableSql);

                await Create(client, createTableSql, config.WriteTimeout);

                Logger.LogInformation("Created table {ResourcePathYdb}", config.ResourcePathYdb);

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
            tasks[i] = Upsert(client, config);
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

    protected abstract Task Create(T client, string createTableSql, int operationTimeout);

    public async Task Run(RunConfig runConfig)
    {
        // Trace.Listeners.Add(new ConsoleTraceListener()); debug meterPusher

        var promPgwEndpoint = $"{runConfig.PromPgw}/metrics";
        var client = await CreateClient(runConfig);
        using var prometheus = new MetricPusher(promPgwEndpoint, "workload-" + Job,
            intervalMilliseconds: runConfig.ReportPeriod);
        prometheus.Start();

        var (_, _, maxId) = await Select(client, $"SELECT MAX(id) as max_id FROM `{runConfig.ResourcePathYdb}`;",
            new Dictionary<string, YdbValue>(), runConfig.ReadTimeout);
        _maxId = (int)maxId!;

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

        var writeTask = ShootingTask(writeLimiter, "write", Upsert);
        var readTask = ShootingTask(readLimiter, "read", Select);

        Logger.LogInformation("Started write / read shooting..");

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
            Func<T, RunConfig, Counter?, Task<(int, StatusCode)>> action)
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
                        pendingOperations.Inc();
                        var sw = Stopwatch.StartNew();
                        var (attempts, statusCode) = await action(client, runConfig, errorsTotal);
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
                    }, cancellationTokenSource.Token);
                }

                Logger.LogInformation("{ShootingName} shooting is stopped", operationType);
            });
        }
    }

    // return attempt count & StatusCode operation
    protected abstract Task<(int, StatusCode)> Upsert(T client, string upsertSql,
        Dictionary<string, YdbValue> parameters,
        int writeTimeout, Counter? errorsTotal = null);

    protected abstract Task<(int, StatusCode, object?)> Select(T client, string selectSql,
        Dictionary<string, YdbValue> parameters, int readTimeout, Counter? errorsTotal = null);

    private Task<(int, StatusCode)> Upsert(T client, Config config, Counter? errorsTotal = null)
    {
        const int minSizeStr = 20;
        const int maxSizeStr = 40;

        return Upsert(client,
            $"""
             DECLARE $id AS Int32;
             DECLARE $payload_str AS Utf8;
             DECLARE $payload_double AS Double;
             DECLARE $payload_timestamp AS Timestamp;
             UPSERT INTO `{config.ResourcePathYdb}` (id, hash, payload_str, payload_double, payload_timestamp)
             VALUES ($id, Digest::NumericHash($id), $payload_str, $payload_double, $payload_timestamp)
             """, new Dictionary<string, YdbValue>
            {
                { "$id", YdbValue.MakeInt32(Interlocked.Increment(ref _maxId)) },
                {
                    "$payload_str", YdbValue.MakeUtf8(string.Join(string.Empty, Enumerable
                        .Repeat(0, Random.Shared.Next(minSizeStr, maxSizeStr))
                        .Select(_ => (char)Random.Shared.Next(127))))
                },
                { "$payload_double", YdbValue.MakeDouble(Random.Shared.NextDouble()) },
                { "$payload_timestamp", YdbValue.MakeTimestamp(DateTime.Now) }
            }, config.WriteTimeout, errorsTotal);
    }

    protected abstract Task<T> CreateClient(Config config);

    private async Task<(int, StatusCode)> Select(T client, RunConfig config, Counter? errorsTotal = null)
    {
        var (attempts, code, _) = await Select(client,
            $"""
             DECLARE $id AS Int32;
             SELECT id, payload_str, payload_double, payload_timestamp, payload_hash
             FROM `{config.ResourcePathYdb}` WHERE id = $id AND hash = Digest::NumericHash($id)
             """,
            new Dictionary<string, YdbValue>
            {
                { "$id", YdbValue.MakeInt32(Random.Shared.Next(_maxId)) }
            }, config.ReadTimeout, errorsTotal);

        return (attempts, code);
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