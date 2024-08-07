using Ydb.Sdk.GrpcWrappers.Topic;
using Ydb.Sdk.Services.Topic;

namespace Ydb.Sdk.Tests.Fixture;

public class RawTopicClientFixture : DriverFixture
{
    internal RawTopicClient TopicClient { get; }

    public RawTopicClientFixture()
    {
        TopicClient = new RawTopicClient(Driver);
    }

    protected override void ClientDispose()
    {
    }
}
