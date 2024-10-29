using System.Text;

namespace Ydb.Sdk.Services.Topic.Reader;

public class ReaderBuilder<TValue>
{
    private readonly Driver _driver;

    public ReaderBuilder(Driver driver)
    {
        _driver = driver;
    }

    public IDeserializer<TValue>? Deserializer { get; set; }

    /// <summary>
    /// Message that describes topic to read.
    /// Topics that will be read by this reader.
    /// </summary>
    public List<SubscribeSettings> SubscribeSettings { get; } = new();

    /// <summary>
    /// Path of consumer that is used for reading by this session.
    /// </summary>
    public string? ConsumerName { get; set; }

    /// <summary>
    /// Optional name. Will be shown in debug stat.
    /// </summary>
    public string? ReaderName { get; set; }

    /// <summary>
    /// Maximum amount of data the broker shall return for a Fetch request.
    /// Messages are fetched in batches by the consumer and if the first message batch
    /// in the first non-empty partition of the Fetch request is larger than this value,
    /// then the message batch will still be returned to ensure the consumer can make progress.
    /// </summary>
    public long MemoryUsageMaxBytes { get; set; } = 50 * 1024 * 1024; // 50 Mb

    public IReader<TValue> Build()
    {
        var config = new ReaderConfig(
            subscribeSettings: SubscribeSettings,
            consumerName: ConsumerName,
            readerName: ReaderName,
            memoryUsageMaxBytes: MemoryUsageMaxBytes
        );

        var reader = new Reader<TValue>(
            _driver,
            config,
            Deserializer ?? (IDeserializer<TValue>)(
                Deserializers.DefaultDeserializers.TryGetValue(typeof(TValue), out var deserializer)
                    ? deserializer
                    : throw new ReaderException("The deserializer is not set")
            )
        );

        return reader;
    }
}

public class SubscribeSettings
{
    public string TopicPath { get; }

    /// <param name="topicPath">Topic path</param>
    public SubscribeSettings(string topicPath)
    {
        TopicPath = topicPath;
    }

    /// <summary>
    /// Partitions that will be read by this session.
    /// If list is empty - then session will read all partitions.
    /// </summary>
    public List<long> PartitionIds { get; } = new();

    /// <summary>
    /// Skip all messages that has write timestamp smaller than now - max_lag.
    /// Zero means infinite lag.
    /// </summary>
    public TimeSpan? MaxLag { get; set; }

    /// <summary>
    /// Read data only after this timestamp from this topic.
    /// Read only messages with 'written_at' value greater or equal than this timestamp.
    /// </summary>
    public DateTime? ReadFrom { get; set; }

    public override string ToString()
    {
        var toString = new StringBuilder().Append("{TopicPath: ").Append(TopicPath);

        if (MaxLag != null)
        {
            toString.Append(", MaxLog: ").Append(MaxLag);
        }

        if (ReadFrom != null)
        {
            toString.Append(", ReadFrom: ").Append(ReadFrom);
        }

        toString.Append("PartitionIds: [").Append(string.Join(", ", PartitionIds)).Append("]}");

        return toString.ToString();
    }
}
