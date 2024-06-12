using Ydb.Sdk.GrpcWrappers.Topic.Reader.Batch;

namespace Ydb.Sdk.Services.Topic.Models.Reader;

public class Message
{
    public long SequenceNumber { get; set; }
    public DateTime CreatedAt { get; set; }
    public string MessageGroupId { get; set; } = null!;
    public Dictionary<string, string>? SessionMetadata { get; set; }
    public long Offset { get; set; }
    public DateTime WrittenAt { get; set; }
    public string ProducerId { get; set; } = null!;
    public Dictionary<string, byte[]> Data { get; set; } = new();

    internal static Message FromWrapper(MessageData message)
    {
        return new Message
        {

        };
    }
}
