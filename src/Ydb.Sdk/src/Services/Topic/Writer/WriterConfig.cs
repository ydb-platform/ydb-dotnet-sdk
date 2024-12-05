using System.Text;

namespace Ydb.Sdk.Services.Topic.Writer;

internal class WriterConfig
{
    internal WriterConfig(
        string topicPath,
        string? producerId,
        Codec codec,
        int bufferMaxSize,
        long? partitionId,
        int bufferOverflowRetryTimeoutMs)
    {
        TopicPath = topicPath;
        ProducerId = producerId;
        Codec = codec;
        BufferMaxSize = bufferMaxSize;
        PartitionId = partitionId;
        BufferOverflowRetryTimeoutMs = bufferOverflowRetryTimeoutMs;
    }

    public string TopicPath { get; }

    public string? ProducerId { get; }

    public Codec Codec { get; }

    public int BufferMaxSize { get; }

    public long? PartitionId { get; }

    public int BufferOverflowRetryTimeoutMs { get; }

    public override string ToString()
    {
        var toString = new StringBuilder().Append("[TopicPath: ").Append(TopicPath);

        if (ProducerId != null)
        {
            toString.Append(", ProducerId: ").Append(ProducerId);
        }

        return toString.Append(", Codec: ").Append(Codec).Append(']').ToString();
    }
}
