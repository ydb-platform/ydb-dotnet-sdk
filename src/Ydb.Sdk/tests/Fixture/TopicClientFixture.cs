using Ydb.Sdk.Services.Topic;

namespace Ydb.Sdk.Tests.Fixture;

public class TopicClientFixture: DriverFixture
{
    public TopicClient TopicClient { get; }

    public TopicClientFixture()
    {
        TopicClient = new TopicClient(Driver);
    }

    protected override void ClientDispose()
    {
    }
}
