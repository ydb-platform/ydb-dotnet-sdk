using Ydb.Sdk.Client;

namespace Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.DropTopic;

internal class DropTopicResponse
{
    public ClientOperation Operation { get; private set; } = null!;

    public static DropTopicResponse FromProto(Ydb.Topic.DropTopicResponse response)
    {
        return new DropTopicResponse
        {
            Operation = ClientOperation.FromProto(response.Operation)
        };
    }
}
