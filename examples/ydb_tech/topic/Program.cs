using System.Text;
using Ydb.Sdk.Topic;
using Ydb.Sdk.Topic.Reader;
using Ydb.Sdk.Topic.Writer;

var connectionString = Environment.GetEnvironmentVariable("YDB_CONNECTION_STRING")
                       ?? "Host=localhost;Port=2136;Database=/local";
var topicName = "ydb_tech_" + Guid.NewGuid().ToString("N");
var consumers = new[] { "one", "batch", "commit_one", "commit_batch", "selectors" };
var expected = new HashSet<string> { "buffered", "acknowledged", "deadline", "metadata" };
using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(60));
var createdTopics = new List<string>();

// [BEGIN topic_init]
await using var topicClient = new TopicClient(connectionString);
// [END topic_init]

try
{
    // [BEGIN topic_create]
    var settings = new CreateTopicSettings
    {
        Path = topicName,
        PartitioningSettings = new PartitioningSettings { MinActivePartitions = 3, MaxActivePartitions = 3 }
    };
    foreach (var consumerName in consumers)
    {
        settings.Consumers.Add(new Consumer(consumerName));
    }
    await topicClient.CreateTopic(settings);
    createdTopics.Add(topicName);
    // [END topic_create]

    await topicClient.CreateTopic(new CreateTopicSettings
    {
        Path = topicName + "_another",
        Consumers = { new Consumer("selectors") }
    });
    createdTopics.Add(topicName + "_another");

    // [BEGIN topic_start_writer]
    await using (var writer = new WriterBuilder<string>(connectionString, topicName)
                 { ProducerId = "ydb-tech-producer" }.Build())
    // [END topic_start_writer]
    {
        // [BEGIN topic_write]
        var pendingWrite = writer.WriteAsync("buffered", deadline.Token);
        // [END topic_write]
        var result = await pendingWrite;
        if (result.Status != PersistenceStatus.Written)
        {
            throw new InvalidOperationException("The first message was not written");
        }

        // [BEGIN topic_write_ack]
        await writer.WriteAsync("acknowledged", deadline.Token);
        // [END topic_write_ack]

        // [BEGIN topic_write_deadline]
        using var writeDeadline = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await writer.WriteAsync("deadline", writeDeadline.Token);
        // [END topic_write_deadline]

        // [BEGIN topic_write_metadata]
        await writer.WriteAsync(new Ydb.Sdk.Topic.Writer.Message<string>("metadata")
        {
            Metadata = { new Metadata("meta-key", Encoding.UTF8.GetBytes("meta-value")) }
        }, deadline.Token);
        // [END topic_write_metadata]
    }

    await ReadOne("one", false);
    await ReadBatch("batch", false);
    await ReadOne("commit_one", true);
    await ReadBatch("commit_batch", true);

    // [BEGIN topic_reader_selectors]
    await using (var reader = new ReaderBuilder<string>(connectionString)
    {
        ConsumerName = "selectors",
        SubscribeSettings =
        {
            new SubscribeSettings(topicName),
            new SubscribeSettings(topicName + "_another") { ReadFrom = DateTime.UnixEpoch }
        }
    }.Build())
    // [END topic_reader_selectors]
    {
        var message = await reader.ReadAsync(deadline.Token);
        CheckPayload(message.Data);
    }
}
finally
{
    foreach (var path in createdTopics)
    {
        // [BEGIN topic_drop]
        await topicClient.DropTopic(path);
        // [END topic_drop]
    }
}

Console.WriteLine("All topic scenarios completed");

async Task ReadOne(string consumerName, bool commit)
{
    // [BEGIN topic_start_reader]
    await using var reader = new ReaderBuilder<string>(connectionString)
    {
        ConsumerName = consumerName,
        SubscribeSettings = { new SubscribeSettings(topicName) }
    }.Build();
    // [END topic_start_reader]

    var received = new HashSet<string>();
    if (!commit)
    {
        // [BEGIN topic_read_one]
        while (received.Count < expected.Count)
        {
            var message = await reader.ReadAsync(deadline.Token);
            CheckPayload(message.Data);
            received.Add(message.Data);
        }
        // [END topic_read_one]
    }
    else
    {
        // [BEGIN topic_read_commit]
        while (received.Count < expected.Count)
        {
            var message = await reader.ReadAsync(deadline.Token);
            CheckPayload(message.Data);
            received.Add(message.Data);
            await message.CommitAsync();
        }
        // [END topic_read_commit]
    }
    if (!received.SetEquals(expected))
    {
        throw new InvalidOperationException("The reader did not receive the expected messages");
    }
}

async Task ReadBatch(string consumerName, bool commit)
{
    await using var reader = new ReaderBuilder<string>(connectionString)
    {
        ConsumerName = consumerName,
        SubscribeSettings = { new SubscribeSettings(topicName) }
    }.Build();
    var received = new HashSet<string>();
    if (!commit)
    {
        // [BEGIN topic_read_batch]
        while (received.Count < expected.Count)
        {
            var batch = await reader.ReadBatchAsync(deadline.Token);
            foreach (var message in batch.Batch)
            {
                CheckPayload(message.Data);
                received.Add(message.Data);
            }
        }
        // [END topic_read_batch]
    }
    else
    {
        // [BEGIN topic_read_batch_commit]
        while (received.Count < expected.Count)
        {
            var batch = await reader.ReadBatchAsync(deadline.Token);
            foreach (var message in batch.Batch)
            {
                CheckPayload(message.Data);
                received.Add(message.Data);
            }
            await batch.CommitBatchAsync();
        }
        // [END topic_read_batch_commit]
    }
    if (!received.SetEquals(expected))
    {
        throw new InvalidOperationException("The reader did not receive the expected messages");
    }
}

void CheckPayload(string data)
{
    if (!expected.Contains(data))
    {
        throw new InvalidOperationException("Unexpected message: " + data);
    }
}
