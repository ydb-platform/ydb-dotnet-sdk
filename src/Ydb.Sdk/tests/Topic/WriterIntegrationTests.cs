using Google.Protobuf.WellKnownTypes;
using Xunit;
using Ydb.Sdk.Services.Topic;
using Ydb.Sdk.Services.Topic.Writer;
using Ydb.Sdk.Tests.Fixture;
using Ydb.Topic;
using Ydb.Topic.V1;
using Consumer = Ydb.Sdk.Services.Topic.Consumer;

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

        using var writer = new WriterBuilder<string>(_driver, _topicName) { ProducerId = "producerId" }.Build();

        var result = await writer.WriteAsync("abacaba");

        Assert.Equal(PersistenceStatus.Written, result.Status);

        await topicClient.DropTopic(new DropTopicSettings { Path = _topicName });
    }

    [Fact]
    public async Task WriteAsync_WhenTopicNotFound_ReturnNotFoundException()
    {
        using var writer = new WriterBuilder<string>(_driver, _topicName + "_not_found")
            { ProducerId = "producerId" }.Build();

        Assert.Contains(
            $"Initialization failed: Status: SchemeError, Issues:\n[500017] Error: no path 'local/{_topicName + "_not_found"}'",
            (await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync("hello world"))).Message
        );
    }

    [Fact]
    public async Task WriteAsync_When1000Messages_ReturnWriteResultIsPersisted()
    {
        const int messageCount = 1000;
        var topicName = _topicName + "_stress";
        var topicClient = new TopicClient(_driver);
        var topicSettings = new CreateTopicSettings { Path = topicName };
        topicSettings.Consumers.Add(new Consumer("Consumer"));
        await topicClient.CreateTopic(topicSettings);

        using var writer = new WriterBuilder<int>(_driver, topicName) { ProducerId = "producerId" }.Build();

        var tasks = new List<Task>();
        for (var i = 0; i < messageCount; i++)
        {
            var i1 = i;
            tasks.Add(Task.Run(async () =>
            {
                // ReSharper disable once AccessToDisposedClosure
                var message = await writer.WriteAsync(i1);

                Assert.Equal(PersistenceStatus.Written, message.Status);
            }));
        }

        await Task.WhenAll(tasks);

        var initStream = _driver.BidirectionalStreamCall(TopicService.StreamReadMethod, new GrpcRequestSettings());
        await initStream.Write(new StreamReadMessage.Types.FromClient
        {
            InitRequest = new StreamReadMessage.Types.InitRequest
            {
                Consumer = "Consumer", ReaderName = "reader-test", TopicsReadSettings =
                {
                    new StreamReadMessage.Types.InitRequest.Types.TopicReadSettings
                        { ReadFrom = new Timestamp(), Path = topicName }
                }
            }
        });

        var ans = 0;

        await initStream.MoveNextAsync();
        await initStream.Write(new StreamReadMessage.Types.FromClient
        {
            ReadRequest = new StreamReadMessage.Types.ReadRequest
            {
                BytesSize = 2000 * messageCount * sizeof(int)
            }
        });
        await initStream.MoveNextAsync();
        var startRequest = initStream.Current.StartPartitionSessionRequest;
        await initStream.Write(new StreamReadMessage.Types.FromClient
        {
            StartPartitionSessionResponse = new StreamReadMessage.Types.StartPartitionSessionResponse
            {
                CommitOffset = startRequest.CommittedOffset,
                PartitionSessionId = startRequest.PartitionSession.PartitionSessionId
            }
        });
        var receivedMessageCount = 0;
        while (receivedMessageCount < messageCount)
        {
            await initStream.MoveNextAsync();
            ans += initStream.Current.ReadResponse.PartitionData.Sum(data => data.Batches.Sum(batch =>
                batch.MessageData.Sum(message =>
                {
                    receivedMessageCount++;
                    return Deserializers.Int32.Deserialize(message.Data.ToByteArray());
                })));
        }

        Assert.Equal(messageCount * (messageCount - 1) / 2, ans);

        await topicClient.DropTopic(new DropTopicSettings { Path = topicName });
    }
}
