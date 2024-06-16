using Ydb.Sdk.GrpcWrappers.Topic.Writer;
using Ydb.Sdk.GrpcWrappers.Topic.Writer.Init;
using Ydb.Sdk.Services.Topic.Models;

namespace Ydb.Sdk.Services.Topic;

public class WriterConfig
{
    public string Topic { get; set; } = null!;
    public string? ProducerId { get; set; }
    public Dictionary<string, string>? SessionMetaData { get; set; }
    public long? PartitionId { get; set; }
    public bool AutoSetSequenceNumber { get; set; }
    public bool AutoSetCreatedAt { get; set; }
    public Codec? Codec { get; set; }
    public Dictionary<Codec, Func<byte[], byte[]>>? Encoders { get; set; }

    internal InitRequest ToInitRequest()
    {
        ProducerId ??= Guid.NewGuid().ToString();
        return new InitRequest
        {
            Path = Topic,
            ProducerId = ProducerId,
            WriteSessionMeta = SessionMetaData ?? new Dictionary<string, string>(),
            Partitioning = GetPartitioning(),
            NeedLastSequenceNumber = true
        };
    }

    private Partitioning GetPartitioning()
    {
        if (PartitionId.HasValue)
        {
            return new Partitioning
            {
                Type = PartitioningType.PartitionId,
                PartitionId = PartitionId.Value
            };
        }

        return new Partitioning
        {
            Type = PartitioningType.MessageGroupId,
            MessageGroupId = ProducerId!
        };
    }
}
