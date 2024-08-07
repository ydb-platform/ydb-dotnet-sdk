using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.CreateTopic;

// ReSharper disable once CheckNamespace
namespace Ydb.Sdk.Services.Topic.Options;

internal class CreateOption: IOption<CreateTopicRequest>
{
    private readonly Action<CreateTopicRequest> _apply;

    public CreateOption(Action<CreateTopicRequest> apply)
    {
        _apply = apply;
    }

    public void Apply(CreateTopicRequest request)
    {
        _apply(request);
    }
}