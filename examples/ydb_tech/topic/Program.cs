using Ydb.Sdk.Topic;
using Ydb.Sdk.Topic.Reader;
using Ydb.Sdk.Topic.Writer;

var connectionString = Environment.GetEnvironmentVariable("YDB_CONNECTION_STRING")
                       ?? "Host=localhost;Port=2136;Database=/local";
await using var cleanupClient = new TopicClient(connectionString);
var createdTopics = new List<string>();
try
{
    var topicName = "ydb_tech_" + Guid.NewGuid().ToString("N");
    await Create(topicName);
    await Initialize(topicName);
    await Write(topicName);
    await ReadOne(topicName, false);
    await ReadBatch(topicName, false);
    await ReadOne(topicName, true);
    var batchTopic = topicName + "_batch";
    await Create(batchTopic);
    await Seed(batchTopic);
    await ReadBatch(batchTopic, true);
    var selectorTopic = topicName + "_selectors";
    await Create(selectorTopic);
    await Create(selectorTopic + "_another");
    await Seed(selectorTopic);
    await Selectors(selectorTopic);
}
finally
{
    foreach (var topicName in createdTopics)
    {
        await Drop(topicName);
    }
}
Console.WriteLine("All topic scenarios completed");

async Task Initialize(string topicName)
{
    // [BEGIN topic_init]
    await using var topicClient = new TopicClient(connectionString);

    await using var writer = new WriterBuilder<string>(connectionString, topicName)
    {
        ProducerId = "ProducerId_Example"
    }.Build();

    await using var reader = new ReaderBuilder<string>(connectionString)
    {
        ConsumerName = "Consumer_Example",
        SubscribeSettings = { new SubscribeSettings(topicName) }
    }.Build();
    // [END topic_init]
}

async Task Create(string topicName)
{
    var topicClient = cleanupClient;
    // [BEGIN topic_create]
    await topicClient.CreateTopic(new CreateTopicSettings
    {
        Path = topicName,
        Consumers = { new Consumer("Consumer_Example") },
        SupportedCodecs = { Codec.Raw, Codec.Gzip },
        PartitioningSettings = new PartitioningSettings
        {
            MinActivePartitions = 3
        }
    });
    // [END topic_create]
    createdTopics.Add(topicName);
}

async Task Drop(string topicName)
{
    var topicClient = cleanupClient;
    // [BEGIN topic_drop]
    await topicClient.DropTopic(topicName);
    // [END topic_drop]
}

async Task Write(string topicName)
{
    // [BEGIN topic_start_writer]
    await using var writer = new WriterBuilder<string>(connectionString, topicName)
    {
        ProducerId = "ProducerId_Example"
    }.Build();
    // [END topic_start_writer]
    // [BEGIN topic_write]
    var asyncWriteTask = writer.WriteAsync("Hello, Example YDB Topics!"); // Task<WriteResult>
    // [END topic_write]
    var result = await asyncWriteTask;
    if (result.Status != PersistenceStatus.Written)
        throw new InvalidOperationException("The first message was not written");
    // [BEGIN topic_write_ack]
    await writer.WriteAsync("Hello, Example YDB Topics!");
    // [END topic_write_ack]
    // [BEGIN topic_write_deadline]
    var writeCts = new CancellationTokenSource();
    writeCts.CancelAfter(TimeSpan.FromSeconds(3));

    await writer.WriteAsync("Hello, Example YDB Topics!", writeCts.Token);
    // [END topic_write_deadline]
    writeCts.Dispose();
    // [BEGIN topic_write_metadata]
    await writer.WriteAsync(
        new Ydb.Sdk.Topic.Writer.Message<string>("Hello, Example YDB Topics!")
            { Metadata = { new Metadata("meta-key", "meta-value"u8.ToArray()) } }
    );
    // [END topic_write_metadata]
}

async Task Seed(string topicName)
{
    await using var writer = new WriterBuilder<string>(connectionString, topicName)
    {
        ProducerId = "seed"
    }.Build();
    for (var i = 0; i < 4; i++)
        await writer.WriteAsync("Hello, Example YDB Topics!");
}

async Task Selectors(string topicName)
{
    // [BEGIN topic_reader_selectors]
    await using var reader = new ReaderBuilder<string>(connectionString)
    {
        ConsumerName = "Consumer_Example",
        SubscribeSettings =
        {
            new SubscribeSettings(topicName),
            new SubscribeSettings(topicName + "_another") { ReadFrom = DateTime.Now }
        }
    }.Build();
    // [END topic_reader_selectors]
    using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
    var message = await reader.ReadAsync(deadline.Token);
    if (message.Data != "Hello, Example YDB Topics!")
        throw new InvalidOperationException("Unexpected selector payload");
}

async Task ReadOne(string topicName, bool commit)
{
    // [BEGIN topic_start_reader]
    await using var reader = new ReaderBuilder<string>(connectionString)
    {
        ConsumerName = "Consumer_Example",
        SubscribeSettings = { new SubscribeSettings(topicName) }
    }.Build();
    // [END topic_start_reader]
    using var readerCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
    var logger = new ExampleLogger(readerCts, 4);
    if (!commit)
    {
        // [BEGIN topic_read_one]
        try
        {
            while (!readerCts.IsCancellationRequested)
            {
                var message = await reader.ReadAsync(readerCts.Token);

                logger.LogInformation("Received message: [{MessageData}]", message.Data);
            }
        }
        catch (OperationCanceledException)
        {
        }
        // [END topic_read_one]
    }
    else
    {
        // [BEGIN topic_read_commit]
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
                    logger.LogError(e, "Failed to commit a message");
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        // [END topic_read_commit]
    }
    logger.RequireComplete();
}

async Task ReadBatch(string topicName, bool commit)
{
    await using var reader = new ReaderBuilder<string>(connectionString)
    {
        ConsumerName = "Consumer_Example",
        SubscribeSettings = { new SubscribeSettings(topicName) }
    }.Build();
    using var readerCts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
    var logger = new ExampleLogger(readerCts, 4);
    if (!commit)
    {
        // [BEGIN topic_read_batch]
        try
        {
            while (!readerCts.IsCancellationRequested)
            {
                var batchMessages = await reader.ReadBatchAsync(readerCts.Token);

                foreach (var message in batchMessages.Batch)
                {
                    logger.LogInformation("Received message: [{MessageData}]", message.Data);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        // [END topic_read_batch]
    }
    else
    {
        // [BEGIN topic_read_batch_commit]
        try
        {
            while (!readerCts.IsCancellationRequested)
            {
                var batchMessages = await reader.ReadBatchAsync(readerCts.Token);

                foreach (var message in batchMessages.Batch)
                {
                    logger.LogInformation("Received message: [{MessageData}]", message.Data);
                }

                try
                {
                    await batchMessages.CommitBatchAsync();
                }
                catch (ReaderException e)
                {
                    logger.LogError(e, "Failed to commit a message");
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        // [END topic_read_batch_commit]
    }
    logger.RequireComplete();
}

sealed class ExampleLogger(CancellationTokenSource cancellation, int expected)
{
    private int _received;

    public void LogInformation(string format, string data)
    {
        if (data != "Hello, Example YDB Topics!")
            throw new InvalidOperationException("Unexpected message: " + data);
        Console.WriteLine(format.Replace("{MessageData}", data));
        if (++_received == expected)
            cancellation.Cancel();
    }

    public void LogError(Exception error, string message) =>
        throw new InvalidOperationException(message, error);

    public void RequireComplete()
    {
        if (_received != expected)
            throw new InvalidOperationException($"Expected {expected} messages, received {_received}");
    }
}
