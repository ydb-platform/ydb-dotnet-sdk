using Ydb.Sdk.GrpcWrappers.Topic.Codecs;

namespace Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.AlterTopic;

internal class AlterConsumer
{
    public string Name { get; set; }
    public bool? IsImportant { get; set; }
    public DateTime? ReadFrom { get; set; }
    public SupportedCodecs SupportedCodecs { get; set; }
    public Dictionary<string, string> AlterAttributes { get; set; }

    public Ydb.Topic.AlterConsumer ToProto()
    {
        //TODO
        return new Ydb.Topic.AlterConsumer
        {

        };
    }
}
