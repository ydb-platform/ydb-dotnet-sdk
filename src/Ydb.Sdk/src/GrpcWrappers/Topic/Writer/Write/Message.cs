using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using static Ydb.Topic.StreamWriteMessage.Types.WriteRequest;

namespace Ydb.Sdk.GrpcWrappers.Topic.Writer.Write;

internal class Message: HasPartitioning<Types.MessageData>
{
    public long SequenceNumber { get; set; }
    public DateTime CreatedAt { get; set; }
    public long UncompressedSize { get; set; }
    public Partitioning Partitioning { get; set; } = null!; 
    public byte[] Data { get; set; } = null!;

    public Types.MessageData ToProto()
    {
        var result = new Types.MessageData
        {
            SeqNo = SequenceNumber,
            CreatedAt = Timestamp.FromDateTime(CreatedAt),
            Data = ByteString.CopyFrom(Data),
            UncompressedSize = UncompressedSize,
        };
        SetPartitioningToProto(result, Partitioning);

        return result;
    }

    protected override void SetEmptyPartitioning(Types.MessageData result) => result.ClearPartitioning();

    protected override void SetMessageGroupId(Types.MessageData result, string messageGroupId)
        => result.MessageGroupId = messageGroupId;

    protected override void SetPartitionId(Types.MessageData result, long partitionId)
        => result.PartitionId = partitionId;
}
