using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.DescribeTopic;
using Ydb.Sdk.Services.Topic.Internal.Options;

namespace Ydb.Sdk.Services.Topic.Options;

internal class DescribeOption: IOption<DescribeTopicRequest>
{
    private readonly Action<DescribeTopicRequest> apply;

    public DescribeOption(Action<DescribeTopicRequest> apply)
    {
        this.apply = apply;
    }

    public void Apply(DescribeTopicRequest request)
    {
        apply(request);
    }
}