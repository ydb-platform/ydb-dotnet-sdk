using Google.Protobuf.WellKnownTypes;
using Ydb.Sdk.GrpcWrappers.Topic.Codecs;
using Ydb.Sdk.GrpcWrappers.Topic.Extensions;

namespace Ydb.Sdk.GrpcWrappers.Topic.ControlPlane;

internal class Consumer
{
    public string Name { get; set; } = null!;
    public bool IsImportant { get; set; }
    public SupportedCodecs SupportedCodecs { get; set; } = null!;
    public DateTime? ReadFrom { get; set; }
    public Dictionary<string, string> Attributes { get; set; } = new();

    public static Consumer FromProto(Ydb.Topic.Consumer consumer)
    {
        return new Consumer
        {
            Name = consumer.Name,
            IsImportant = consumer.Important,
            Attributes = consumer.Attributes.ToDictionary(),
            ReadFrom = consumer.ReadFrom == default ? null : consumer.ReadFrom.ToDateTime(),
            SupportedCodecs = SupportedCodecs.FromProto(consumer.SupportedCodecs)
        };
    }

    public Ydb.Topic.Consumer ToProto()
    {
        return new Ydb.Topic.Consumer
        {
            Name = Name,
            Important = IsImportant,
            ReadFrom = ReadFrom.HasValue ? Timestamp.FromDateTime(ReadFrom.Value) : default,
            SupportedCodecs = SupportedCodecs.ToProto(),
            Attributes = {Attributes}
        };
    }
}
