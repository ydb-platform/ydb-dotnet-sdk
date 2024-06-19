using Ydb.Sdk.Client;

namespace Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.CreateTopic;

internal class CreateTopicResponse
{
    public ClientOperation Operation { get; set; } = null!;

    public static CreateTopicResponse FromProto(Ydb.Topic.CreateTopicResponse response)
    {
        return new CreateTopicResponse
        {
            Operation = ClientOperation.FromProto(response.Operation)
        };
    }
}
