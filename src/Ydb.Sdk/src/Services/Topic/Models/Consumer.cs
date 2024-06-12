namespace Ydb.Sdk.Services.Topic.Models;

public class Consumer
{
    public string Name { get; set; } = null!;
    public bool IsImportant { get; set; }
    public List<Codec> SupportedCodecs { get; set; } = new();
    public DateTime? ReadFrom { get; set; }
    public Dictionary<string, string> Attributes { get; set; } = new();

    internal GrpcWrappers.Topic.ControlPlane.Consumer ToWrapper()
    {
        return new GrpcWrappers.Topic.ControlPlane.Consumer
        {
            Name = Name,
            IsImportant = IsImportant,
            ReadFrom = ReadFrom,
            Attributes = Attributes,
            SupportedCodecs = GrpcWrappers.Topic.Codecs.SupportedCodecs.FromPublic(SupportedCodecs)
        };
    }

    internal Consumer FromWrapper(GrpcWrappers.Topic.ControlPlane.Consumer consumer)
    {
        return new Consumer
        {
            Name = Name,
            IsImportant = IsImportant,
            ReadFrom = ReadFrom,
            Attributes = Attributes,
            SupportedCodecs = SupportedCodecs
        };
    }
}