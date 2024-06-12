using Ydb.Sdk.GrpcWrappers.Topic.Codecs;

namespace Ydb.Sdk.GrpcWrappers.Topic.Reader.Batch;

internal class Batch
{
    public Codec Codec { get; set; }
    public string ProducerId { get; set; } = null!;
    public Dictionary<string, string>? WriteSessionMeta { get; set; }
    public DateTime WrittenAt { get; set; }
    public List<MessageData> MessageData { get; set; } = null!;
}