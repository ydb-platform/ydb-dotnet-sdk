namespace Ydb.Sdk.Services.Topic.Models.Writer;

public class Message
{
    public long SequenceNumber { get; set; }
    public DateTime CreatedAt { get; set; }
    public byte[] Data { get; set; }
    public Dictionary<string, byte[]> MetaData { get; set; } = new();

    internal GrpcWrappers.Topic.Writer.Write.Message ToWrapper()
    {
        return new GrpcWrappers.Topic.Writer.Write.Message
        {
            Codec = GrpcWrappers.Topic.Codecs.Codec.Raw,
            SequenceNumber = SequenceNumber,
            CreatedAt = CreatedAt,
            Data = Data,
            UncompressedSize = Data.Length
        };
    }
}
