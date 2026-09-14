using System.Text;

namespace Ydb.Sdk.Topic.Writer;

internal class WriterConfig
{
    internal WriterConfig(
        string topicPath,
        string? producerId,
        string? writerName,
        Codec codec,
        int bufferMaxSize,
        long? partitionId)
    {
        TopicPath = topicPath;
        ProducerId = producerId;
        WriterName = writerName;
        Codec = codec;
        BufferMaxSize = bufferMaxSize;
        PartitionId = partitionId;
    }

    public string TopicPath { get; }

    public string? ProducerId { get; }

    public string? WriterName { get; }

    public Codec Codec { get; }

    public int BufferMaxSize { get; }

    public long? PartitionId { get; }

    public override string ToString()
    {
        var toString = new StringBuilder().Append("TopicPath: ").Append(TopicPath);

        if (ProducerId != null)
        {
            toString.Append(", ProducerId: ").Append(ProducerId);
        }

        if (PartitionId != null)
        {
            toString.Append(", PartitionId: ").Append(PartitionId);
        }

        return toString.Append(", Codec: ").Append(Codec).ToString();
    }
}
