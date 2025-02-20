using System.Collections.Concurrent;
using System.Threading.RateLimiting;
using Internal;
using Microsoft.Extensions.Logging;
using Ydb.Sdk;
using Ydb.Sdk.Services.Topic;
using Ydb.Sdk.Services.Topic.Reader;
using Ydb.Sdk.Services.Topic.Writer;

namespace TopicService;

public class SloTopicContext : ISloContext
{
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
                    {
                        Important = true
                    }
                }
            }
        );

        Logger.LogInformation("Topic[{TopicName}] created!", PathTopic);
    }

    public async Task Run(RunConfig config)
    {
        AppContext.SetSwitch("System.Net.SocketsHttpHandler.Http2FlowControl.DisableDynamicWindowSizing", true);
        
        Logger.LogInformation("Started Run topic slo test");
        var driver = await Driver.CreateInitialized(
            new DriverConfig(config.Endpoint, config.Db), ISloContext.Factory);

        Logger.LogInformation("Driver is initialized!");

        var writeLimiter = new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
        {
            Window = TimeSpan.FromMilliseconds(100), PermitLimit = config.WriteRps / 10, QueueLimit = int.MaxValue
        });

        var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromSeconds(config.Time));

        var messageSending = new ConcurrentDictionary<long, ConcurrentQueue<string>>();

        var writeTasks = new List<Task>();
        for (var i = 0; i < PartitionSize; i++)
        {
            var partitionId = i;
            messageSending[partitionId] = new ConcurrentQueue<string>();

            writeTasks.Add(
                Task.Run(async () =>
                {
                    try
                    {
                        using var writer = new WriterBuilder<string>(driver, PathTopic)
                        {
                            BufferMaxSize = 8 * 1024 * 1024,
                            ProducerId = "producer-" + partitionId,
                            PartitionId = partitionId
                        }.Build();

                        Logger.LogInformation("Started Writer[PartitionId={PartitionId}]", partitionId);

                        var messageNum = 1;
                        while (!cts.IsCancellationRequested)
                        {
                            using var lease = await writeLimiter.AcquireAsync(cancellationToken: cts.Token);
                            using var writeRpc = new CancellationTokenSource();
                            writeRpc.CancelAfter(TimeSpan.FromSeconds(config.WriteTimeout));

                            if (!lease.IsAcquired)
                            {
                                continue;
                            }

                            var data = $"message-{messageNum++}";
                            messageSending[partitionId].Enqueue(data);

                            await writer.WriteAsync(data, writeRpc.Token);
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        Logger.LogInformation("Finished Writer[PartitionId={PartitionId}]", partitionId);
                    }
                    catch (WriterException e)
                    {
                        Logger.LogCritical(e, "Failed Writer[PartitionId={PartitionId}]", partitionId);

                        await cts.CancelAsync();

                        throw;
                    }
                }, cts.Token)
            );
        }

        var readTasks = new List<Task>();
        for (var i = 0; i < PartitionSize; i++)
        {
            var handlerBatch = i % 2 == 0;
            var partitionId = i;
            readTasks.Add(Task.Run(async () =>
            {
                try
                {
                    using var reader = new ReaderBuilder<string>(driver)
                    {
                        ConsumerName = ConsumerName,
                        SubscribeSettings =
                        {
                            new SubscribeSettings(PathTopic)
                            {
                                PartitionIds = { partitionId }
                            }
                        },
                        MemoryUsageMaxBytes = 8 * 1024 * 1024,
                    }.Build();

                    Logger.LogInformation("Started Reader[PartitionId={PartitionId}]", partitionId);

                    if (handlerBatch)
                    {
                        await ReadBatchMessages(cts, reader, messageSending, partitionId);
                    }
                    else
                    {
                        await ReadMessage(cts, reader, messageSending, partitionId);
                    }
                }
                catch (OperationCanceledException)
                {
                    Logger.LogInformation("Finished Reader[PartitionId={PartitionId}]", partitionId);
                }
                catch (Exception e)
                {
                    Logger.LogCritical(e, "Failed SLO test");

                    await cts.CancelAsync();
                }
            }, cts.Token));
        }

        await Task.WhenAll(writeTasks);
        await Task.WhenAll(readTasks);

        Logger.LogInformation("Task finish!");
    }

    private static async Task ReadBatchMessages(
        CancellationTokenSource cts,
        IReader<string> reader,
        ConcurrentDictionary<long, ConcurrentQueue<string>> localStore,
        int partitionId
    )
    {
        var queryFailedCommited = new Queue<string>();
        var prevSuccessCommitMessage = "Nothing";
        while (!cts.IsCancellationRequested)
        {
            var batchMessages = await reader.ReadBatchAsync(cts.Token);

            foreach (var message in batchMessages.Batch)
            {
                while (queryFailedCommited.TryDequeue(out var expectedMessageData))
                {
                    Logger.LogInformation(
                        "ReadBatchMessages[PartitionId={PartitionId}] has repeated read: {MessageData}", partitionId,
                        expectedMessageData);

                    if (expectedMessageData == message.Data)
                    {
                        goto ContinueForeach;
                    }
                    
                    if (localStore.TryGetValue(message.PartitionId, out var expectedQueue))
                    {
                        if (expectedQueue.TryPeek(out var commitedMessage))
                        {
                            if (commitedMessage == message.Data)
                            {
                                expectedQueue.TryDequeue(out _);
                                
                                goto ContinueForeach;
                            }
                        }
                    }

                    if (string.CompareOrdinal(expectedMessageData, message.Data) < 0)
                    {
                        continue;
                    }

                    Logger.LogCritical("Previous success messages: {PrevSuccessCommitMessage}. \n" +
                                       "FAILED ReadBatchMessages prevFailMessage is greater than message data! \n" +
                                       "Local store: {LocalStore}",
                        prevSuccessCommitMessage, PrintLocalStore(localStore));

                    AssertMessage(message, expectedMessageData);
                }

                CheckMessage(localStore, message);

                ContinueForeach: ;
            }

            try
            {
                await batchMessages.CommitBatchAsync();

                prevSuccessCommitMessage = string.Join(", ", batchMessages.Batch.Select(m =>
                    $"[Topic: {m.Topic}, Data: {m.Data}, PartitionId: {m.PartitionId}, CreatedAt: {m.CreatedAt}]"));
            }
            catch (ReaderException e)
            {
                Logger.LogInformation(e, "Previous success messages: {PrevSuccessCommitMessage}. \n" +
                                         "Commit batch have readerException error! For messages: {Messages} \n" +
                                         "Local store: {LocalStore}",
                    prevSuccessCommitMessage, string.Join(", ", batchMessages.Batch.Select(m =>
                        $"[Topic: {m.Topic}, Data: {m.Data}, PartitionId: {m.PartitionId}, CreatedAt: {m.CreatedAt}]")),
                    PrintLocalStore(localStore));

                foreach (var message in batchMessages.Batch)
                {
                    queryFailedCommited.Enqueue(message.Data);
                }
            }
        }
    }

    private static async Task ReadMessage(
        CancellationTokenSource cts,
        IReader<string> reader,
        ConcurrentDictionary<long, ConcurrentQueue<string>> localStore,
        int partitionId
    )
    {
        string? prevFailMessage = null;
        var prevSuccessCommitMessage = "Nothing";

        while (!cts.IsCancellationRequested)
        {
            var message = await reader.ReadAsync(cts.Token);

            if (prevFailMessage != null)
            {
                Logger.LogInformation("ReadMessage[PartitionId={PartitionId}] has repeated read: {MessageData}",
                    partitionId, prevFailMessage);

                if (string.CompareOrdinal(prevFailMessage, message.Data) > 0)
                {
                    Logger.LogCritical("Previous success messages: {PrevSuccessCommitMessage}. \n" +
                                       "FAILED ReadMessage prevFailMessage is greater than message data! \n" +
                                       "Local store: {LocalStore}",
                        prevSuccessCommitMessage, PrintLocalStore(localStore));

                    AssertMessage(message, prevFailMessage);
                }

                prevFailMessage = null;

                if (localStore.TryGetValue(message.PartitionId, out var expectedQueue))
                {
                    if (expectedQueue.TryPeek(out var commitedMessage))
                    {
                        if (commitedMessage == message.Data)
                        {
                            expectedQueue.TryDequeue(out _);
                        }
                    }
                }

                goto ContinueForeach;
            }

            CheckMessage(localStore, message);

            ContinueForeach:
            try
            {
                await message.CommitAsync();

                prevSuccessCommitMessage = $"[Topic: {message.Topic}, Data: {message.Data}, " +
                                           $"PartitionId: {message.PartitionId}, CreatedAt: {message.CreatedAt}]";
            }
            catch (ReaderException e)
            {
                Logger.LogInformation(e,
                    "Commit message have ReaderException error! For message: " +
                    "[Topic: {Topic}, Data: {Data}, PartitionId: {PartitionId}, CreatedAt: {CreatedAt}] \n" +
                    "Previous success messages: {PrevSuccessCommitMessage}. \n" +
                    "Local store: {LocalStore}",
                    message.Topic, message.Data, message.PartitionId, message.CreatedAt, prevSuccessCommitMessage,
                    PrintLocalStore(localStore));

                prevFailMessage = message.Data;
            }
        }
    }

    private static void CheckMessage(ConcurrentDictionary<long, ConcurrentQueue<string>> localStore,
        Ydb.Sdk.Services.Topic.Reader.Message<string> message)
    {
        if (localStore.TryGetValue(message.PartitionId, out var partition))
        {
            if (partition.TryDequeue(out var expectedMessageData))
            {
                AssertMessage(message, expectedMessageData);
                return;
            }

            Logger.LogCritical(
                "Unknown message: [Topic: {Topic}, Data: {Data}, PartitionId: {PartitionId}, CreatedAt: {CreatedAt}]\n" +
                "Local store: {LocalStore}",
                message.Topic, message.Data, message.PartitionId, message.CreatedAt, PrintLocalStore(localStore));

            throw new Exception("FAILED SLO TEST: UNKNOWN MESSAGE!");
        }

        Logger.LogCritical(
            "Unknown message: [Topic: {Topic}, Data: {Data}, PartitionId: {PartitionId}, CreatedAt: {CreatedAt}]\n" +
            "Local store: {LocalStore}",
            message.Topic, message.Data, message.PartitionId, message.CreatedAt, PrintLocalStore(localStore));

        throw new Exception("FAILED SLO TEST: NOT FOUND PARTITION FOR PRODUCER_ID!");
    }

    private static string PrintLocalStore(ConcurrentDictionary<long, ConcurrentQueue<string>> localStore)
    {
        return "[" +
               string.Join("\n", localStore.Select(pair => pair.Key + ": " + string.Join(", ", pair.Value))) +
               "]";
    }

    private static void AssertMessage(Ydb.Sdk.Services.Topic.Reader.Message<string> message, string expectedMessageData)
    {
        if (expectedMessageData == message.Data)
        {
            return;
        }

        Logger.LogCritical(
            "Fail assertion messages! expectedData: {ExpectedData}, " +
            "actualMessage: [Topic: {Topic}, Data: {Data}, PartitionId: {PartitionId}, CreatedAt: {CreatedAt}]",
            expectedMessageData, message.Topic, message.Data, message.PartitionId, message.CreatedAt);

        throw new Exception("FAILED SLO TEST: ASSERT ERROR!");
    }
}