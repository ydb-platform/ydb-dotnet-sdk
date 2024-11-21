using Xunit;
using Ydb.Sdk.Services.Topic;
using Ydb.Sdk.Services.Topic.Writer;
using Ydb.Sdk.Tests.Fixture;

namespace Ydb.Sdk.Tests.Topic;

public class WriterIntegrationTests : IClassFixture<DriverFixture>
{
    private readonly IDriver _driver;
    private readonly string _topicName;

    public WriterIntegrationTests(DriverFixture driverFixture)
    {
        _driver = driverFixture.Driver;
        _topicName = "topic_" + Utils.Net;
    }

    [Fact]
    public async Task WriteAsync_WhenOneMessage_ReturnWritten()
    {
        var topicClient = new TopicClient(_driver);
        var topicSettings = new CreateTopicSettings
        {
            Path = _topicName
        };
        await topicClient.CreateTopic(topicSettings);

        using var writer = new WriterBuilder<string>(_driver,
                new WriterConfig(_topicName) { ProducerId = "producerId" })
            .Build();

        var result = await writer.WriteAsync("abacaba");

        Assert.Equal(PersistenceStatus.Written, result.Status);

        await topicClient.DropTopic(new DropTopicSettings { Path = _topicName });
    }

    [Fact]
    public async Task WriteAsync_WhenTopicNotFound_ReturnNotFoundException()
    {
        using var writer = new WriterBuilder<string>(_driver, new WriterConfig(_topicName + "_not_found")
            { ProducerId = "producerId" }).Build();

        Assert.Equal(StatusCode.SchemeError, (await Assert.ThrowsAsync<WriterException>(
            () => writer.WriteAsync("hello world"))).Status.StatusCode);
    }
}
