using Microsoft.Extensions.Logging;
using Ydb.Sdk;
using Ydb.Sdk.Services.Topic;
using Ydb.Sdk.Services.Topic.Reader;
using Ydb.Sdk.Services.Topic.Writer;

const int countMessages = 100;
const string topicName = "topic_name";

var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));

var logger = loggerFactory.CreateLogger<Program>();

var config = new DriverConfig(
    endpoint: "grpc://localhost:2136",
    database: "/local"
);

await using var driver = await Driver.CreateInitialized(
    config: config,
    loggerFactory: loggerFactory
);

var topicClient = new TopicClient(driver);

await topicClient.CreateTopic(new CreateTopicSettings
{
    Path = topicName,
    Consumers = { new Consumer("Consumer_Example") }
});

var readerCts = new CancellationTokenSource();

var writerJob = Task.Run(async () =>
{
    // ReSharper disable once AccessToDisposedClosure
    await using var writer = new WriterBuilder<string>(driver, topicName)
    {
        ProducerId = "ProducerId_Example"
    }.Build();

    for (var i = 0; i < countMessages; i++)
    {
        await writer.WriteAsync($"Message num {i}: Hello Example YDB Topics!");
    }

    readerCts.CancelAfter(TimeSpan.FromSeconds(3));
});

var readerJob = Task.Run(async () =>
{
    // ReSharper disable once AccessToDisposedClosure
    await using var reader = new ReaderBuilder<string>(driver)
    {
        ConsumerName = "Consumer_Example",
        SubscribeSettings = { new SubscribeSettings(topicName) }
    }.Build();

    try
    {
        while (!readerCts.IsCancellationRequested)
        {
            var message = await reader.ReadAsync(readerCts.Token);

            logger.LogInformation("Received message: [{MessageData}]", message.Data);

            try
            {
                await message.CommitAsync();
            }
            catch (Exception e)
            {
                logger.LogError(e, "Failed commit message");
            }
        }
    }
    catch (OperationCanceledException)
    {
    }
});

await writerJob;
await readerJob;
await topicClient.DropTopic(new DropTopicSettings { Path = topicName });