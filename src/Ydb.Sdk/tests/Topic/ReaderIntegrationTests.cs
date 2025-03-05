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
        _topicName = "reader_topic_" + Utils.Net;
    }

    [Fact]
    public async Task StressTest_WhenReadingThenCommiting_ReturnMessages()
    {
        var topicClient = new TopicClient(_driver);
        var topicSettings = new CreateTopicSettings { Path = _topicName };
        topicSettings.Consumers.Add(new Consumer("Consumer"));
        await topicClient.CreateTopic(topicSettings);

        await using var writer = new WriterBuilder<string>(_driver, _topicName)
            { ProducerId = "producerId" }.Build();
        var reader = new ReaderBuilder<string>(_driver)
        {
            ConsumerName = "Consumer",
            SubscribeSettings = { new SubscribeSettings(_topicName) },
            MemoryUsageMaxBytes = 200
        }.Build();

        for (var i = 0; i < 100; i++)
        {
            await writer.WriteAsync($"{i}: Hello World!");
            var message = await reader.ReadAsync();
            Assert.Equal($"{i}: Hello World!", message.Data);
            await message.CommitAsync();
        }

        await reader.DisposeAsync();

        var readerNext = new ReaderBuilder<string>(_driver)
        {
            ConsumerName = "Consumer",
            SubscribeSettings = { new SubscribeSettings(_topicName) },
            MemoryUsageMaxBytes = 1000
        }.Build();

        for (var i = 100; i < 200; i++)
        {
            await writer.WriteAsync($"{i}: Hello World!");
            var message = await readerNext.ReadAsync();
            Assert.Equal($"{i}: Hello World!", message.Data);
            await message.CommitAsync();
        }

        await readerNext.DisposeAsync();

        await topicClient.DropTopic(new DropTopicSettings { Path = _topicName });
    }
}
