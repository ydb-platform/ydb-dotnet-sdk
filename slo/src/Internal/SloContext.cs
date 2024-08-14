using System.Diagnostics;
using Internal.Cli;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.RateLimit;
using Prometheus;
using Ydb.Sdk;
using Ydb.Sdk.Value;

namespace Internal;

public abstract class SloContext<T> where T : IDisposable
{
    protected readonly ILoggerFactory Factory;
    private readonly ILogger _logger;

    private volatile int _maxId;

    protected SloContext()
    {
        Factory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));
        _logger = Factory.CreateLogger<SloContext<T>>();
    }

    protected abstract string JobName { get; }

    public async Task Create(CreateConfig config)
    {
        const int maxCreateAttempts = 10;

        using var client = await CreateClient(config);
        for (var attempt = 0; attempt < maxCreateAttempts; attempt++)
        {
            _logger.LogInformation("Creating table {TableName}..", config.TableName);
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
                _logger.LogInformation("YQL script: {sql}", createTableSql);

                await Create(client, createTableSql, config.WriteTimeout);

                _logger.LogInformation("Created table {TableName}", config.TableName);

                break;
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Fail created table");

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
            _logger.LogError(e, "Init failed when all tasks, continue..");
        }
        finally
        {
            _logger.LogInformation("Created task is finished");
        }
    }

    protected abstract Task Create(T client, string createTableSql, int operationTimeout);

    public async Task Run(RunConfig runConfig)
    {
        var promPgwEndpoint = $"{runConfig.PromPgw}/metrics";
        var client = await CreateClient(runConfig);
        using var prometheus = new MetricPusher(promPgwEndpoint, JobName, intervalMilliseconds: runConfig.ReportPeriod);
        prometheus.Start();

        var (_, _, maxId) = await Select(client, $"SELECT MAX(id) as max_id FROM `{runConfig.TableName}`;",
            new Dictionary<string, YdbValue>(), runConfig.ReadTimeout);
        _maxId = (int)maxId!;

        _logger.LogInformation("Init row count: {MaxId}", _maxId);

        var metricFactory = Metrics.WithLabels(new Dictionary<string, string>
            { { "jobName", JobName }, { "sdk", "dotnet" }, { "sdkVersion", Environment.Version.ToString() } });

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

        var writeLimiter = Policy.RateLimit(runConfig.WriteRps, TimeSpan.FromSeconds(1), runConfig.WriteRps);
        var readLimiter = Policy.RateLimit(runConfig.ReadRps, TimeSpan.FromSeconds(1), runConfig.ReadRps);

        var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(runConfig.ShutdownTime));

        var writeTask = ShootingTask(writeLimiter, "write", Upsert);
        var readTask = ShootingTask(readLimiter, "read", Select);

        _logger.LogInformation("Started write / read shooting..");

        await Task.WhenAll(readTask, writeTask);

        await prometheus.StopAsync();
        await MetricReset(promPgwEndpoint);

        _logger.LogInformation("Run task is finished");
        return;

        Task ShootingTask(RateLimitPolicy rateLimitPolicy, string shootingName,
            Func<T, RunConfig, Gauge?, Task<(int, StatusCode)>> action)
        {
            return Task.Run(async () =>
            {
                var tasks = new List<Task>();

                long activeTasks = 0;

                while (!cancellationTokenSource.Token.IsCancellationRequested)
                {
                    try
                    {
                        tasks.Add(rateLimitPolicy.Execute(async () =>
                        {
                            // ReSharper disable once AccessToModifiedClosure
                            Interlocked.Increment(ref activeTasks);

                            var sw = Stopwatch.StartNew();
                            var (attempts, statusCode) = await action(client, runConfig, errorsGauge);
                            string label;

                            if (statusCode != StatusCode.Success)
                            {
                                _logger.LogError("Failed {ShootingName} operation code: {StatusCode}", shootingName,
                                    statusCode);
                                notOkGauge.Inc();
                                label = "err";
                            }
                            else
                            {
                                okGauge.Inc();
                                label = "ok";
                            }

                            Interlocked.Decrement(ref activeTasks);
                            attemptsHistogram.WithLabels(label).Observe(attempts);
                            latencySummary.WithLabels(label).Observe(sw.ElapsedMilliseconds);
                        }));
                    }
                    catch (RateLimitRejectedException e)
                    {
                        _logger.LogInformation(e, "Waiting {ShootingName} task, count active tasks: {}", shootingName,
                            Interlocked.Read(ref activeTasks));

                        await Task.Delay(e.RetryAfter, cancellationTokenSource.Token);
                    }
                }

                await Task.WhenAll(tasks);

                _logger.LogInformation("{ShootingName} shooting is stopped", shootingName);
            }, cancellationTokenSource.Token);
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
                { "$id", YdbValue.MakeUint64((ulong)Random.Shared.Next(_maxId)) }
            }, config.ReadTimeout, errorsGauge);

        return (attempts, code);
    }

    private async Task MetricReset(string promPgwEndpoint)
    {
        var deleteUri = $"{promPgwEndpoint}/job/{JobName}";
        using var httpClient = new HttpClient();
        await httpClient.DeleteAsync(deleteUri);
    }
}