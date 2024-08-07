using Ydb.Operations;

namespace Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.DescribeTopic;

internal class DescribeTopicRequest
{
    public string Path { get; set; } = null!;
    public OperationSettings? OperationSettings { get; set; }
    //TODO include stats?

    public Ydb.Topic.DescribeTopicRequest ToProto()
    {
        return new Ydb.Topic.DescribeTopicRequest
        {
            Path = Path,
            OperationParams = OperationSettings?.MakeOperationParams() ?? new OperationParams(),
        };
    }
}
