using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Threading.RateLimiting;
using Internal;
using Microsoft.Extensions.Logging;
using Prometheus;
using Ydb.Sdk;
using Ydb.Sdk.Services.Topic;
using Ydb.Sdk.Services.Topic.Reader;
using Ydb.Sdk.Services.Topic.Writer;

namespace TopicService;

public class SloTopicContext : ISloContext
{
    private const string Job = "TopicService";
    private const string PathTopic = "/Root/testdb/slo-topic";
    private const string ConsumerName = "Consumer";
    private const int PartitionSize = 10;

    private static readonly ILogger Logger = ISloContext.Factory.CreateLogger<SloTopicContext>();

    public async Task Create(CreateConfig config)
    {
        var topicClient = new TopicClient(await Driver.CreateInitialized(
            new DriverConfig(config.Endpoint, config.Db), ISloContext.Factory)
        );

        await topicClient.CreateTopic(
            new CreateTopicSettings
            {
                Path = PathTopic,
                PartitioningSettings = new PartitioningSettings
                {
                    MinActivePartitions = PartitionSize
                },
                Consumers =
                {
                    new Consumer(ConsumerName)
                }
            }
        );

        Logger.LogInformation("Topic[{TopicName}] created!", PathTopic);
    }

    public async Task Run(RunConfig config)
    {
        var driver = await Driver.CreateInitialized(
            new DriverConfig(config.Endpoint, config.Db), ISloContext.Factory);
        var promPgwEndpoint = $"{config.PromPgw}/metrics";
        using var prometheus = new MetricPusher(promPgwEndpoint, "workload-" + Job,
            intervalMilliseconds: config.ReportPeriod);
        prometheus.Start();

        var writeLimiter = new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
        {
            Window = TimeSpan.FromMilliseconds(100), PermitLimit = config.WriteRps / 10, QueueLimit = int.MaxValue
        });

        var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromSeconds(config.Time));

        var messageSending = new ConcurrentDictionary<string, ConcurrentQueue<string>>();

        var writeTasks = new List<Task>();
        for (var i = 0; i < PartitionSize; i++)
        {
            const string operationType = "write";
            var producer = "producer-" + (i + 1);
            messageSending[producer] = new ConcurrentQueue<string>();

            writeTasks.Add(
                Task.Run(async () =>
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

                    var operationsSuccessTotal = metricFactory.CreateCounter(
                        "sdk_operations_success_total",
                        "Total number of successful operations, categorized by type."
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

                    using var writer = new WriterBuilder<string>(driver, PathTopic)
                    {
                        ProducerId = producer,
                        BufferMaxSize = 8 * 1024 * 1024
                    }.Build();

                    Logger.LogInformation("Started Writer[ProducerId={ProducerId}]", producer);

                    while (!cts.IsCancellationRequested)
                    {
                        using var lease = await writeLimiter.AcquireAsync(cancellationToken: cts.Token);

                        if (!lease.IsAcquired)
                        {
                            continue;
                        }

                        var textBuilder = new StringBuilder();

                        var size = Random.Shared.Next(1, 100);
                        for (var j = 0; j < size; j++)
                        {
                            textBuilder.Append(Guid.NewGuid());
                        }

                        var data = textBuilder.ToString();
                        messageSending[producer].Enqueue(data);

                        var sw = Stopwatch.StartNew();
                        // ReSharper disable once MethodSupportsCancellation
                        await writer.WriteAsync(data);
                        sw.Stop();

                        operationsSuccessTotal.Inc();
                        operationLatencySeconds.WithLabels("success").Observe(sw.Elapsed.TotalSeconds);
                    }
                }, cts.Token)
            );
        }

        var readTasks = new List<Task>();
        for (var i = 0; i < PartitionSize; i++)
        {
            var handlerBatch = i % 2 == 0;
            const string operationType = "read";
            var number = i;
            readTasks.Add(Task.Run(async () =>
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

                var operationsSuccessTotal = metricFactory.CreateCounter(
                    "sdk_operations_success_total",
                    "Total number of successful operations, categorized by type."
                );

                using var reader = new ReaderBuilder<string>(driver)
                {
                    ConsumerName = ConsumerName,
                    SubscribeSettings =
                    {
                        new SubscribeSettings(PathTopic)
                    },
                    MemoryUsageMaxBytes = 8 * 1024 * 1024,
                }.Build();

                Logger.LogInformation("Started Reader[{Number}]", number);

                if (handlerBatch)
                {
                    await ReadBatchMessages(cts, reader, messageSending, operationsSuccessTotal);
                }
                else
                {
                    await ReadMessage(cts, reader, messageSending, operationsSuccessTotal);
                }
            }, cts.Token));
        }

        try
        {
            await Task.WhenAll(writeTasks);
            await Task.WhenAll(readTasks);
        }
        catch (OperationCanceledException)
        {
        }

        await prometheus.StopAsync();
        
        Logger.LogInformation("Task finish!");
    }

    private static async Task ReadBatchMessages(
        CancellationTokenSource cts,
        IReader<string> reader,
        ConcurrentDictionary<string, ConcurrentQueue<string>> localStore,
        Counter messageCounter
    )
    {
        while (!cts.IsCancellationRequested)
        {
            var batchMessages = await reader.ReadBatchAsync(cts.Token);
            messageCounter.Inc(batchMessages.Batch.Count);

            foreach (var message in batchMessages.Batch)
            {
                CheckMessage(localStore, message);
            }
        }
    }

    private static async Task ReadMessage(
        CancellationTokenSource cts,
        IReader<string> reader,
        ConcurrentDictionary<string, ConcurrentQueue<string>> localStore,
        Counter messageCounter
    )
    {
        while (!cts.IsCancellationRequested)
        {
            CheckMessage(localStore, await reader.ReadAsync(cts.Token));
            messageCounter.Inc();
        }
    }

    private static void CheckMessage(ConcurrentDictionary<string, ConcurrentQueue<string>> localStore,
        Ydb.Sdk.Services.Topic.Reader.Message<string> message)
    {
        if (localStore.TryGetValue(message.ProducerId, out var partition))
        {
            if (partition.TryDequeue(out var expectedMessageData))
            {
                if (expectedMessageData == message.Data)
                {
                    return;
                }

                Logger.LogCritical(
                    "Fail assertion messages! expectedData: {ExpectedData}, " +
                    "actualMessage: [Topic: {Topic}, Data: {Data}, ProducerId: {ProducerId}, CreatedAt: {CreatedAt}]",
                    expectedMessageData, message.Topic, message.Data, message.ProducerId, message.CreatedAt);

                throw new Exception("FAILED SLO TEST: ASSERT ERROR!");
            }

            Logger.LogCritical(
                "Unknown message: [Topic: {Topic}, ProducerId: {ProducerId}, CreatedAt: {CreatedAt}]",
                message.Topic, message.ProducerId, message.CreatedAt);

            throw new Exception("FAILED SLO TEST: UNKNOWN MESSAGE!");
        }

        Logger.LogCritical(
            "Unknown message: [Topic: {Topic}, ProducerId: {ProducerId}, CreatedAt: {CreatedAt}]",
            message.Topic, message.ProducerId, message.CreatedAt);

        throw new Exception("FAILED SLO TEST: NOT FOUND PARTITION FOR PRODUCER_ID!");
    }
}