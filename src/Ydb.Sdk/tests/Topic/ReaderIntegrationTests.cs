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
    private readonly TopicClient _topicClient;

    public ReaderIntegrationTests(DriverFixture driverFixture)
    {
        _driver = driverFixture.Driver;
        _topicName = $"reader{Random.Shared.Next()}_topic_" + Utils.Net;
        _topicClient = new TopicClient(_driver);
    }

    [Fact]
    public async Task StressTest_WhenReadingThenCommiting_ReturnMessages()
    {
        var topicSettings = new CreateTopicSettings { Path = _topicName };
        topicSettings.Consumers.Add(new Consumer("Consumer"));
        await _topicClient.CreateTopic(topicSettings);

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

        await _topicClient.DropTopic(_topicName);
    }

    // Fixed error on receiving the biggest messages
    // Grpc.Core.RpcException: Status(StatusCode="ResourceExhausted", Detail="Received message exceeds the maximum configured message size.")
    [Fact]
    public async Task BigMessage_WhenClientSendingLargeMessage_ReturnReading()
    {
        const int messageSize = 100_000;
        const int payloadSize = 200;

        var topicSettings = new CreateTopicSettings { Path = _topicName };
        topicSettings.Consumers.Add(new Consumer("Consumer"));
        await _topicClient.CreateTopic(topicSettings);
        await using var writer = new WriterBuilder<byte[]>(_driver, _topicName)
            { ProducerId = "producerId" }.Build();

        var payload = new byte[payloadSize];
        Random.Shared.NextBytes(payload);

        // 20 Mb sending
        for (var i = 0; i < messageSize; i++)
        {
            await writer.WriteAsync(payload);
        }

        await using var reader = new ReaderBuilder<byte[]>(_driver)
        {
            ConsumerName = "Consumer",
            SubscribeSettings = { new SubscribeSettings(_topicName) }
        }.Build();


        // 20 Mb reading
        for (var i = 0; i < messageSize; i++)
        {
            var message = await reader.ReadAsync();
            Assert.Equal(payload, message.Data);
            await message.CommitAsync();
        }
    }
}
