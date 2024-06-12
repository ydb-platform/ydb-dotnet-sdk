namespace Ydb.Sdk.GrpcWrappers.Topic.Reader.Batch;

internal class MessageData
{
    public long Offset { get; set; }
    public long SequenceNumber { get; set; }
    public DateTime CreatedAt { get; set; }
    public byte[] Data { get; set; } = null!;
    public long UncompressedSize { get; set; }
    public string MessageGroupId { get; set; } = null!;
}
