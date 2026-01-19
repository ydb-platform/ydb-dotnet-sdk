using Microsoft.Extensions.Logging;
using Ydb.Sdk.Topic;
using Ydb.Sdk.Topic.Reader;
using Ydb.Sdk.Topic.Writer;

const string connectionString = "Host=localhost;Port=2136;Database=/local";
const int countMessages = 100;
const string topicName = "topic_name";

var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));

var logger = loggerFactory.CreateLogger<Program>();

await using var topicClient = new TopicClient(connectionString);

await topicClient.CreateTopic(new CreateTopicSettings
{
    Path = topicName,
    Consumers = { new Consumer("Consumer_Example") }
});

var readerCts = new CancellationTokenSource();

var writerJob = Task.Run(async () =>
{
    // ReSharper disable once AccessToDisposedClosure
    await using var writer = new WriterBuilder<string>(connectionString, topicName)
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
    await using var reader = new ReaderBuilder<string>(connectionString)
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
            catch (ReaderException e)
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
await topicClient.DropTopic(topicName);