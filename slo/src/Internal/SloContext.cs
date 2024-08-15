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
        LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));

    protected static readonly ILogger Logger = Factory.CreateLogger<SloContext<T>>();

    private volatile int _maxId;

    protected abstract string Job { get; }

    public async Task Create(CreateConfig config)
    {
        const int maxCreateAttempts = 10;

        using var client = await CreateClient(config);
        for (var attempt = 0; attempt < maxCreateAttempts; attempt++)
        {
            Logger.LogInformation("Creating table {TableName}..", config.TableName);
            try
            {
                var createTableSql = $"""
                                      CREATE TABLE `{config.TableName}` (
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
                                          AUTO_PARTITIONING_PARTITION_SIZE_MB = {config.PartitionSize},
                                          AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = {config.MinPartitionsCount},
                                          AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = {config.MaxPartitionsCount}
                                      );
                                      """;
                Logger.LogInformation("YQL script: {sql}", createTableSql);

                await Create(client, createTableSql, config.WriteTimeout);

                Logger.LogInformation("Created table {TableName}", config.TableName);

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
        var promPgwEndpoint = $"{runConfig.PromPgw}/metrics";
        var client = await CreateClient(runConfig);
        using var prometheus = new MetricPusher(promPgwEndpoint, Job, intervalMilliseconds: runConfig.ReportPeriod);
        prometheus.Start();

        var (_, _, maxId) = await Select(client, $"SELECT MAX(id) as max_id FROM `{runConfig.TableName}`;",
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
        await MetricReset(promPgwEndpoint);

        Logger.LogInformation("Run task is finished");
        return;

        Task ShootingTask(RateLimiter rateLimitPolicy, string jobName,
            Func<T, RunConfig, Gauge?, Task<(int, StatusCode)>> action)
        {
            var metricFactory = Metrics.WithLabels(new Dictionary<string, string>
                { { "jobName", jobName }, { "sdk", "dotnet" }, { "sdkVersion", Environment.Version.ToString() } });

            var okGauge = metricFactory.CreateGauge("oks", "Count of OK");
            var notOkGauge = metricFactory.CreateGauge("not_oks", "Count of not OK");
            var latencySummary = metricFactory.CreateSummary("latency", "Latencies (OK)", new[] { "status" },
                new SummaryConfiguration
                {
                    MaxAge = TimeSpan.FromSeconds(15), Objectives = new QuantileEpsilonPair[]
                        { new(0.5, 0.05), new(0.99, 0.005), new(0.999, 0.0005) }
                });

            var attemptsHistogram = metricFactory.CreateHistogram("attempts", "summary of amount for request",
                new[] { "status" },
                new HistogramConfiguration { Buckets = Histogram.LinearBuckets(1, 1, 10) });

            var errorsGauge = metricFactory.CreateGauge("errors", "amount of errors", new[] { "class", "in" });
            foreach (var statusCode in Enum.GetValues<StatusCode>())
            {
                errorsGauge.WithLabels(statusCode.StatusName(), "retried").IncTo(0);
                errorsGauge.WithLabels(statusCode.StatusName(), "finally").IncTo(0);
            }

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
                        var sw = Stopwatch.StartNew();
                        var (attempts, statusCode) = await action(client, runConfig, errorsGauge);
                        sw.Stop();
                        string label;

                        if (statusCode != StatusCode.Success)
                        {
                            notOkGauge.Inc();
                            label = "err";
                            errorsGauge.WithLabels(statusCode.StatusName(), "finally").Inc();
                        }
                        else
                        {
                            okGauge.Inc();
                            label = "ok";
                        }

                        attemptsHistogram.WithLabels(label).Observe(attempts);
                        latencySummary.WithLabels(label).Observe(sw.ElapsedMilliseconds);
                    }, cancellationTokenSource.Token);
                }

                // ReSharper disable once MethodSupportsCancellation
                await Task.Delay(TimeSpan.FromSeconds(runConfig.ShutdownTime));

                Logger.LogInformation("{ShootingName} shooting is stopped", jobName);
            });
        }
    }

    // return attempt count & StatusCode operation
    protected abstract Task<(int, StatusCode)> Upsert(T client, string upsertSql,
        Dictionary<string, YdbValue> parameters,
        int writeTimeout, Gauge? errorsGauge = null);

    protected abstract Task<(int, StatusCode, object?)> Select(T client, string selectSql,
        Dictionary<string, YdbValue> parameters, int readTimeout, Gauge? errorsGauge = null);

    private Task<(int, StatusCode)> Upsert(T client, Config config, Gauge? errorsGauge = null)
    {
        const int minSizeStr = 20;
        const int maxSizeStr = 40;

        return Upsert(client,
            $"""
             DECLARE $id AS Int32;
             DECLARE $payload_str AS Utf8;
             DECLARE $payload_double AS Double;
             DECLARE $payload_timestamp AS Timestamp;
             UPSERT INTO `{config.TableName}` (id, hash, payload_str, payload_double, payload_timestamp)
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
            }, config.WriteTimeout, errorsGauge);
    }

    protected abstract Task<T> CreateClient(Config config);

    private async Task<(int, StatusCode)> Select(T client, RunConfig config, Gauge? errorsGauge = null)
    {
        var (attempts, code, _) = await Select(client,
            $"""
             DECLARE $id AS Int32;
             SELECT id, payload_str, payload_double, payload_timestamp, payload_hash
             FROM `{config.TableName}` WHERE id = $id AND hash = Digest::NumericHash($id)
             """,
            new Dictionary<string, YdbValue>
            {
                { "$id", YdbValue.MakeInt32(Random.Shared.Next(_maxId)) }
            }, config.ReadTimeout, errorsGauge);

        return (attempts, code);
    }

    private async Task MetricReset(string promPgwEndpoint)
    {
        var deleteUri = $"{promPgwEndpoint}/job/{Job}";
        using var httpClient = new HttpClient();
        await httpClient.DeleteAsync(deleteUri);
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