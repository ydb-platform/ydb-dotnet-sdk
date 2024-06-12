using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.CreateTopic;
using Ydb.Sdk.Services.Topic.Internal.Options;

namespace Ydb.Sdk.Services.Topic.Options;

internal class CreateOption: IOption<CreateTopicRequest>
{
    private readonly Action<CreateTopicRequest> apply;

    public CreateOption(Action<CreateTopicRequest> apply)
    {
        this.apply = apply;
    }

    public void Apply(CreateTopicRequest request)
    {
        apply(request);
    }
}