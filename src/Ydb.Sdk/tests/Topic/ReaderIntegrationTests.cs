using Xunit;
using Ydb.Sdk.Services.Topic;
using Ydb.Sdk.Services.Topic.Reader;
using Ydb.Sdk.Services.Topic.Writer;
using Ydb.Sdk.Tests.Fixture;

namespace Ydb.Sdk.Tests.Topic;

public class ReaderIntegrationTests : IClassFixture<DriverFixture>
{
    private readonly IDriver _driver;
    private readonly string _topicName;

    public ReaderIntegrationTests(DriverFixture driverFixture)
    {
        _driver = driverFixture.Driver;
        _topicName = "topic_" + Utils.Net;
    }

    [Fact]
    public async Task Simple()
    {
        var topicClient = new TopicClient(_driver);
        var topicSettings = new CreateTopicSettings { Path = _topicName };
        topicSettings.Consumers.Add(new Consumer("Consumer"));
        await topicClient.CreateTopic(topicSettings);

        using var writer = new WriterBuilder<string>(_driver, _topicName)
            { ProducerId = "producerId" }.Build();
        using var reader = new ReaderBuilder<string>(_driver)
        {
            ConsumerName = "Consumer",
            SubscribeSettings = { new SubscribeSettings(_topicName) }
        }.Build();

        await writer.WriteAsync("Hello World!");
        Assert.Equal("Hello World!", (await reader.ReadAsync()).Data);
    }
}
