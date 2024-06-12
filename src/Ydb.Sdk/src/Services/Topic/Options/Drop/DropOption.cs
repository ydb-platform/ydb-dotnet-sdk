using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.DropTopic;
using Ydb.Sdk.Services.Topic.Internal.Options;

namespace Ydb.Sdk.Services.Topic.Options;

internal class DropOption: IOption<DropTopicRequest>
{
    private readonly Action<DropTopicRequest> apply;

    public DropOption(Action<DropTopicRequest> apply)
    {
        this.apply = apply;
    }

    public void Apply(DropTopicRequest request)
    {
        apply(request);
    }
}