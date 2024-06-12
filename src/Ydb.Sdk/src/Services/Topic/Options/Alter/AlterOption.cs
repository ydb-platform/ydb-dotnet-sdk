using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.AlterTopic;
using Ydb.Sdk.Services.Topic.Internal.Options;

namespace Ydb.Sdk.Services.Topic.Options;

internal class AlterOption: IOption<AlterTopicRequest>
{
    private readonly Action<AlterTopicRequest> apply;

    public AlterOption(Action<AlterTopicRequest> apply)
    {
        this.apply = apply;
    }

    public void Apply(AlterTopicRequest request)
    {
        apply(request);
    }
}
