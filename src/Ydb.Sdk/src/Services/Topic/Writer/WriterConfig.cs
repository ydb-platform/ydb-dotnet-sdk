using System.Text;

namespace Ydb.Sdk.Services.Topic.Writer;

public class WriterConfig
{
    /// <param name="topicPath">Full path of topic to write to.</param>
    public WriterConfig(string topicPath)
    {
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
    /// All messages with given pair (producer_id, message_group_id) go to single partition in order of writes.
    /// </summary>
    public string? MessageGroupId { get; set; }

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

    public override string ToString()
    {
        var toString = new StringBuilder().Append("[TopicPath: ").Append(TopicPath);

        if (ProducerId != null)
        {
            toString.Append(", ProducerId: ").Append(ProducerId);
        }

        if (MessageGroupId != null)
        {
            toString.Append(", MessageGroupId: ").Append(MessageGroupId);
        }

        return toString.Append(", Codec: ").Append(Codec).Append(']').ToString();
    }
}
