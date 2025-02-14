using System.Collections.Concurrent;
using System.Text;
using Internal;
using Microsoft.Extensions.Logging;
using Ydb.Sdk;
using Ydb.Sdk.Services.Topic;
using Ydb.Sdk.Services.Topic.Reader;
using Ydb.Sdk.Services.Topic.Writer;

namespace TopicService;

public class SloTopicContext : ISloContext
{
    private const string PathTopic = "/Root/pixcc-slice-db/slo-topic";
    private const string ConsumerName = "Consumer";
    private const int PartitionSize = 10;

    private static readonly ILogger Logger = ISloContext.Factory.CreateLogger<SloTopicContext>();

    public async Task Create(CreateConfig config)
    {
//         var splitEndpoint = config.Endpoint.Split("://");
//         var useTls = splitEndpoint[0] switch
//         {
//             "grpc" => false,
//             "grpcs" => true,
//             _ => throw new ArgumentException("Don't support schema: " + splitEndpoint[0])
//         };
//
//         var host = splitEndpoint[1].Split(":")[0];
//         var port = splitEndpoint[1].Split(":")[1];
//
//         await using var connection = new YdbConnection(new YdbConnectionStringBuilder
//         {
//             UseTls = useTls, Host = host, Port = int.Parse(port), Database = config.Db,
//             LoggerFactory = ISloContext.Factory
//         });
//         await connection.OpenAsync();
//
//         await new YdbCommand(connection)
//         {
//             CommandText = @"
//             CREATE TOPIC `/Root/testdb/slo-topic` (
//                 CONSUMER Consumer
//             ) WITH (min_active_partitions = 10);
//
//             CREATE USER user PASSWORD password;
//             "
//         }.ExecuteNonQueryAsync();

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

        var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromSeconds(config.Time));

        var messageSending = new ConcurrentDictionary<string, ConcurrentQueue<string>>();

        var writeTasks = new List<Task>();
        for (var i = 0; i < PartitionSize; i++)
        {
            var producer = "producer-" + (i + 1);
            messageSending[producer] = new ConcurrentQueue<string>();

            writeTasks.Add(
                Task.Run(async () =>
                {
                    using var writer = new WriterBuilder<string>(driver, PathTopic)
                    {
                        ProducerId = producer,
                        BufferMaxSize = 8 * 1024 * 1024
                    }.Build();

                    while (!cts.IsCancellationRequested)
                    {
                        var tasks = new List<Task>();
                        for (var k = 0; k < 10; k++)
                        {
                            var textBuilder = new StringBuilder();

                            var size = Random.Shared.Next(1, 100);
                            for (var j = 0; j < size; j++)
                            {
                                textBuilder.Append(Guid.NewGuid());
                            }

                            var data = textBuilder.ToString();
                            messageSending[producer].Enqueue(data);
                            // ReSharper disable once MethodSupportsCancellation
                            tasks.Add(writer.WriteAsync(data));
                        }

                        await Task.WhenAll(tasks);
                    }
                }, cts.Token)
            );
        }

        var readTasks = new List<Task>();
        for (var i = 0; i < PartitionSize; i++)
        {
            var handlerBatch = i % 2 == 0;

            readTasks.Add(Task.Run(async () =>
            {
                using var reader = new ReaderBuilder<string>(driver)
                {
                    ConsumerName = ConsumerName,
                    SubscribeSettings =
                    {
                        new SubscribeSettings(PathTopic)
                    },
                    MemoryUsageMaxBytes = 8 * 1024 * 1024,
                }.Build();

                if (handlerBatch)
                {
                    await ReadBatchMessages(cts, reader, messageSending);
                }
                else
                {
                    await ReadMessage(cts, reader, messageSending);
                }
            }, cts.Token));
        }

        await Task.WhenAll(writeTasks);
        await Task.WhenAll(readTasks);
    }

    private static async Task ReadBatchMessages(
        CancellationTokenSource cts,
        IReader<string> reader,
        ConcurrentDictionary<string, ConcurrentQueue<string>> localStore
    )
    {
        while (!cts.IsCancellationRequested)
        {
            var batchMessages = await reader.ReadBatchAsync();

            foreach (var message in batchMessages.Batch)
            {
                CheckMessage(localStore, message);
            }
        }
    }

    private static async Task ReadMessage(
        CancellationTokenSource cts,
        IReader<string> reader,
        ConcurrentDictionary<string, ConcurrentQueue<string>> localStore
    )
    {
        while (!cts.IsCancellationRequested)
        {
            CheckMessage(localStore, await reader.ReadAsync());
        }
    }

    private static void CheckMessage(ConcurrentDictionary<string, ConcurrentQueue<string>> localStore,
        Ydb.Sdk.Services.Topic.Reader.Message<string> message)
    {
        if (localStore.TryGetValue(message.ProducerId, out var partition))
        {
            if (partition.TryDequeue(out var expectedMessageData))
            {
                if (expectedMessageData != message.Data)
                {
                    Logger.LogCritical(
                        "Fail assertion messages! expectedData: {ExpectedData}, " +
                        "actualMessage: [Topic: {Topic}, Data: {Data}, ProducerId: {ProducerId}, CreatedAt: {CreatedAt}]",
                        expectedMessageData, message.Topic, message.Data, message.ProducerId, message.CreatedAt);

                    throw new Exception("FAILED SLO TEST: ASSERT ERROR!");
                }
            }
            else
            {
                Logger.LogCritical(
                    "Unknown message: [Topic: {Topic}, ProducerId: {ProducerId}, CreatedAt: {CreatedAt}]",
                    message.Topic, message.ProducerId, message.CreatedAt);

                throw new Exception("FAILED SLO TEST: UNKNOWN MESSAGE!");
            }
        }
        else
        {
            Logger.LogCritical(
                "Unknown message: [Topic: {Topic}, ProducerId: {ProducerId}, CreatedAt: {CreatedAt}]",
                message.Topic, message.ProducerId, message.CreatedAt);

            throw new Exception("FAILED SLO TEST: NOT FOUND PARTITION FOR PRODUCER_ID!");
        }
    }
}