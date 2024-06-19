using Ydb.Sdk.Client;

namespace Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.CreateTopic;

internal class CreateTopicResponse
{
    public ClientOperation ClientOperation { get; set; } = null!;

    public static CreateTopicResponse FromProto(Ydb.Topic.CreateTopicResponse response)
    {
        return new CreateTopicResponse
        {
            ClientOperation = ClientOperation.FromProto(response.Operation)
        };
    }
}
