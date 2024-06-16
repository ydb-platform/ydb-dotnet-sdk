using System.Text;
using Xunit;
using Ydb.Sdk.Services.Topic;
using Ydb.Sdk.Services.Topic.Models.Writer;
using Ydb.Sdk.Tests.Fixture;

namespace Ydb.Sdk.Tests.Topic;

[Trait("Category", "Integration")]
public class TopicWriterTests : IClassFixture<TopicClientFixture>
{
    private readonly TopicClient _topicClient;

    public TopicWriterTests(TopicClientFixture fixture)
    {
        _topicClient = fixture.TopicClient;
    }

    [Fact]
    public async Task TestWriteMessage()
    {
        var writer = _topicClient.StartWriter("/local/topic");
        await writer.WaitInit();
        var message = new Message
        {
            Data = Encoding.UTF8.GetBytes("content")
        };
        var results = await writer.Write(new List<Message> {message});
        Assert.All(results, result => Assert.Equal(WriteResultStatus.Written, result.Status));
    }
}
