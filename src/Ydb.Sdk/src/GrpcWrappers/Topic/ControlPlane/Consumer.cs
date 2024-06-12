using Google.Protobuf.WellKnownTypes;
using Ydb.Sdk.GrpcWrappers.Extensions;
using Ydb.Sdk.GrpcWrappers.Topic.Codecs;

namespace Ydb.Sdk.GrpcWrappers.Topic.ControlPlane;

internal class Consumer
{
    public string Name { get; set; }
    public bool IsImportant { get; set; }
    public SupportedCodecs SupportedCodecs { get; set; }
    public DateTime? ReadFrom { get; set; }
    public Dictionary<string, string> Attributes { get; set; }

    public void FromProto(Ydb.Topic.Consumer consumer)
    {
        Name = consumer.Name;
        IsImportant = consumer.Important;
        Attributes = consumer.Attributes.ToDictionary();
        //ReadFrom = ;
        SupportedCodecs = SupportedCodecs.FromProto(consumer.SupportedCodecs);
    }

    public Ydb.Topic.Consumer ToProto()
    {
        return new Ydb.Topic.Consumer
        {
            Name = Name,
            Important = IsImportant,
            ReadFrom = ReadFrom.HasValue ? Timestamp.FromDateTime(ReadFrom.Value) : default,
            SupportedCodecs = SupportedCodecs.ToProto(),
            //Attributes = Attributes
        };
    }
}
