using Ydb.Sdk.Client;

namespace Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.AlterTopic;

internal class AlterTopicResponse
{
    public ClientOperation Operation { get; set; } = null!;

    public static AlterTopicResponse FromProto(Ydb.Topic.AlterTopicResponse response)
    {
        return new AlterTopicResponse
        {
            Operation = ClientOperation.FromProto(response.Operation)
        };
    }
}
