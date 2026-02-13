namespace Ydb.Sdk.Ado.TxWriter;

/// <summary>
/// Configuration options for creating a transactional topic writer.
/// </summary>
public class TxWriterOptions
{
    /// <summary>
    /// Gets or sets the maximum buffer size in bytes for pending messages.
    /// Default is 64 MB.
    /// </summary>
    /// <remarks>
    /// When the buffer is full, Write() will throw a <see cref="TxTopicWriterException"/>.
    /// The caller should flush pending messages or implement backpressure.
    /// </remarks>
    public int BufferMaxSize { get; set; } = 64 * 1024 * 1024; // 64 MB

    /// <summary>
    /// Gets or sets the producer identifier for message deduplication.
    /// </summary>
    /// <remarks>
    /// Used for message deduplication by sequence numbers within the transaction.
    /// If not specified, a unique identifier will be generated.
    /// </remarks>
    public string? ProducerId { get; set; }

    /// <summary>
    /// Gets or sets the codec for data compression.
    /// Default is Raw (no compression).
    /// </summary>
    public Topic.Codec Codec { get; set; } = Topic.Codec.Raw;

    /// <summary>
    /// Gets or sets an explicit partition ID to write to.
    /// If not specified, the partition will be selected automatically.
    /// </summary>
    public long? PartitionId { get; set; }
}
