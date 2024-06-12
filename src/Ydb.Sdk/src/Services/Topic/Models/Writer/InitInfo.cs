using Ydb.Sdk.GrpcWrappers.Topic.Codecs;

namespace Ydb.Sdk.Services.Topic.Models.Writer;

public class InitInfo
{
    public long LastSequenceNumber { get; set; }
    public List<Codec> SupportedCodecs { get; set; } = null!;
}
