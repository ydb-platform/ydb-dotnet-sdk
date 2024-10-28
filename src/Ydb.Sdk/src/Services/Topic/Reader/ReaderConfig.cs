namespace Ydb.Sdk.Services.Topic.Reader;

public class ReaderConfig
{
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
    /// Direct reading from a partition node.
    /// </summary>
    public bool DirectRead { get; set; }
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
}
