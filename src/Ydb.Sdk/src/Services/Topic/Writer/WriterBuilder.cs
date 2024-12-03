namespace Ydb.Sdk.Services.Topic.Writer;

public class WriterBuilder<TValue>
{
    private readonly IDriver _driver;

    public WriterBuilder(IDriver driver, string topicPath)
    {
        _driver = driver;
        TopicPath = topicPath;
    }

    /// <summary>
    /// Full path of topic to write to.
    /// </summary>
    public string TopicPath { get; }

    /// <summary>
    /// Producer identifier of client data stream.
    /// Used for message deduplication by sequence numbers.
    /// </summary>
    public string? ProducerId { get; set; }

    /// <summary>
    /// Codec that is used for data compression.
    /// See enum Codec above for values.
    /// </summary>
    public Codec Codec { get; set; } = Codec.Raw; // TODO Supported only Raw

    /// <summary>
    /// Maximum size (in bytes) of all messages batched in one Message Set, excluding protocol framing overhead.
    /// This limit is applied after the first message has been added to the batch,
    /// regardless of the first message's size, this is to ensure that messages that exceed buffer size are produced. 
    /// </summary>
    public int BufferMaxSize { get; set; } = 1024 * 1024; // 1 Mb 

    /// <summary>
    /// Explicit partition id to write to.
    /// </summary>    
    public long? PartitionId { get; set; }

    /// <summary>
    /// The serializer to use to serialize values.
    /// </summary>
    /// <remarks>
    ///     If your value serializer throws an exception, this will be
    ///     wrapped in a WriterException with unspecified status.
    /// </remarks>
    public ISerializer<TValue>? Serializer { get; set; }


    /// <summary>
    /// Represents the timeout duration, in milliseconds, used when a buffer overflow is detected.
    /// This timeout specifies how long the system should wait before attempting to retry the operation.
    /// </summary>
    /// <remarks>
    /// This timeout is important for managing system performance and stability. 
    /// Too short a timeout could lead to rapid retry attempts, potentially causing further resource contention
    /// and degrading system performance. Conversely, too long a timeout might delay processing significantly.
    /// </remarks>
    public int BufferOverflowRetryTimeoutMs { get; set; } = 10;

    public IWriter<TValue> Build()
    {
        var config = new WriterConfig(
            topicPath: TopicPath,
            producerId: ProducerId,
            codec: Codec,
            bufferMaxSize: BufferMaxSize,
            partitionId: PartitionId,
            bufferOverflowRetryTimeoutMs: BufferOverflowRetryTimeoutMs
        );

        return new Writer<TValue>(
            _driver,
            config,
            Serializer ?? (ISerializer<TValue>)(
                Serializers.DefaultSerializers.TryGetValue(typeof(TValue), out var serializer)
                    ? serializer
                    : throw new WriterException("The serializer is not set")
            )
        );
    }
}
