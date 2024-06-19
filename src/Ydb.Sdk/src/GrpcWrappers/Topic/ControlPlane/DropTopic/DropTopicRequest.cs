using Ydb.Operations;

namespace Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.DropTopic;

internal class DropTopicRequest
{
    public string Path { get; set; } = null!;
    public OperationSettings? OperationSettings { get; set; }

    public Ydb.Topic.DropTopicRequest ToProto()
    {
        return new Ydb.Topic.DropTopicRequest
        {
            Path = Path,
            OperationParams = OperationSettings?.MakeOperationParams() ?? new OperationParams()
        };
    }
}
