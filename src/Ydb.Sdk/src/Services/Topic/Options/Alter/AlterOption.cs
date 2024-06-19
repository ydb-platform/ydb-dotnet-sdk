using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.AlterTopic;

// ReSharper disable once CheckNamespace
namespace Ydb.Sdk.Services.Topic.Options;

internal class AlterOption: IOption<AlterTopicRequest>
{
    private readonly Action<AlterTopicRequest> _apply;

    public AlterOption(Action<AlterTopicRequest> apply)
    {
        _apply = apply;
    }

    public void Apply(AlterTopicRequest request)
    {
        _apply(request);
    }
}
