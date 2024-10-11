namespace Ydb.Sdk.Services.Topic;

/// <summary>
/// Create topic request sent from client to server.
/// </summary>
public class CreateTopicSettings : OperationSettings
{
    /// <summary>
    /// Topic path.
    /// </summary>
    public string Path { get; set; } = string.Empty;

    /// <summary>
    /// Settings for partitioning
    /// </summary>
    public PartitioningSettings? PartitioningSettings { get; set; }

    /// <summary>
    /// Retention settings.
    ///
    /// How long data in partition should be stored. Must be greater than 0 and less than limit for this database.
    /// Default limit - 36 hours.
    /// </summary>
    public TimeSpan? RetentionPeriod { get; set; }

    /// <summary>
    /// How much data in partition should be stored. Must be greater than 0 and less than limit for this database.
    /// Zero value means infinite limit.
    /// </summary>
    public long RetentionStorageMb { get; set; }

    /// <summary>
    /// List of allowed codecs for writes.
    /// Writes with codec not from this list are forbidden.
    /// If empty, codec compatibility check for the topic is disabled.
    /// </summary>
    public List<Codec> SupportedCodecs { get; } = new();

    /// <summary>
    /// Partition write speed in bytes per second. Must be less than database limit.
    /// Zero value means default limit: 1 MB per second.
    /// </summary>
    public long PartitionWriteSpeedBytesPerSecond { get; set; }

    /// <summary>
    /// Burst size for write in partition, in bytes. Must be less than database limit.
    /// Zero value means default limit: 1 MB.
    /// </summary>
    public long PartitionWriteBurstBytes { get; set; }

    /// <summary>
    /// User and server attributes of topic. Server attributes starts from "_" and will be validated by server.
    /// </summary>
    public Dictionary<string, string> Attributes { get; set; } = new();

    /// <summary>
    /// List of consumers for this topic.
    /// </summary>
    public List<Consumer> Consumers { get; } = new();

    /// <summary>
    /// Metering mode for the topic in a serverless database.
    /// </summary>
    public MeteringMode MeteringMode { get; set; }
}

/// <summary>
/// Partitioning settings for topic.
/// </summary>
public class PartitioningSettings
{
    /// <summary>
    /// Minimum partition count auto merge would stop working at.
    /// Zero value means default - 1.
    /// </summary>
    public long MinActivePartitions { get; set; }

    /// <summary>
    /// Limit for total partition count, including active (open for write) and read-only partitions.
    /// Zero value means default - 100.
    /// </summary>
    public long PartitionCountLimit { get; set; }
}

/// <summary>
/// Drop topic request sent from client to server.
/// </summary>
public class DropTopicSettings : OperationSettings
{
    /// <summary>
    /// Topic path.
    /// </summary>
    public string Path { get; set; } = string.Empty;
}

/// <summary>
/// Update existing topic request sent from client to server.
/// </summary>
public class AlterTopicSettings : OperationSettings
{
    /// <summary>
    /// Topic path.
    /// </summary>
    public string Path { get; set; } = string.Empty;
    
    
}

public enum Codec
{
    Unspecified = Ydb.Topic.Codec.Unspecified,
    Raw = Ydb.Topic.Codec.Raw,
    Gzip = Ydb.Topic.Codec.Gzip,
    Lzop = Ydb.Topic.Codec.Lzop,
    Zstd = Ydb.Topic.Codec.Zstd
}

/// <summary>
/// Metering mode specifies the method used to determine consumption of resources by the topic.
/// This settings will have an effect only in a serverless database.
/// </summary>
public enum MeteringMode
{
    /// <summary>
    /// Use default 
    /// </summary>
    MeteringModeUnspecified = Ydb.Topic.MeteringMode.Unspecified,

    /// <summary>
    /// Metering based on resource reservation
    /// </summary>
    MeteringModeReservedCapacity = Ydb.Topic.MeteringMode.ReservedCapacity,

    /// <summary>
    /// Metering based on actual consumption. Default.
    /// </summary>
    MeteringModeRequestUnits = Ydb.Topic.MeteringMode.RequestUnits
}

/// <summary>
/// Consumer description
/// </summary>
public class Consumer
{
    /// <param name="name">Must have valid not empty name as a key.</param>
    public Consumer(string name)
    {
        Name = name;
    }

    /// <summary>
    /// Must have valid not empty name as a key.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Consumer may be marked as 'important'. It means messages for this consumer will never expire due to retention.
    /// User should take care that such consumer never stalls, to prevent running out of disk space.
    /// Flag that this consumer is important.
    /// </summary>
    public bool Important { get; set; }

    /// <summary>
    /// All messages with smaller server written_at timestamp will be skipped.
    /// </summary>
    public DateTime? ReadFrom { get; set; }

    /// <summary>
    /// List of supported codecs by this consumer.
    /// supported_codecs on topic must be contained inside this list.
    /// If empty, codec compatibility check for the consumer is disabled.
    /// </summary>
    public List<Codec> SupportedCodecs { get; } = new();

    /// <summary>
    /// Attributes of consumer.
    /// </summary>
    public Dictionary<string, string> Attributes { get; } = new();
}
