using Google.Protobuf.WellKnownTypes;
using Ydb.Sdk.GrpcWrappers.Topic.Codecs;

namespace Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.AlterTopic;

internal class AlterConsumer
{
    public string Name { get; set; } = null!;
    public bool? IsImportant { get; set; }
    public DateTime? ReadFrom { get; set; }
    public SupportedCodecs? SupportedCodecs { get; set; }
    public Dictionary<string, string> AlterAttributes { get; set; } = new();

    public Ydb.Topic.AlterConsumer ToProto()
    {
        var result = new Ydb.Topic.AlterConsumer {Name = Name};
        if (IsImportant.HasValue)
            result.SetImportant = IsImportant.Value;
        if (ReadFrom.HasValue)
            result.SetReadFrom = Timestamp.FromDateTime(ReadFrom.Value);
        if (SupportedCodecs != null)
            result.SetSupportedCodecs = SupportedCodecs.ToProto();
        result.AlterAttributes.Add(AlterAttributes);

        return result;
    }
}
