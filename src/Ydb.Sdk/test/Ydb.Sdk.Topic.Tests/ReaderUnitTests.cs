using System.Text;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Moq;
using Xunit;
using Ydb.Issue;
using Ydb.Sdk.Services.Topic;
using Ydb.Sdk.Services.Topic.Reader;
using Ydb.Topic;
using Range = Moq.Range;

namespace Ydb.Sdk.Topic.Tests;

using ReaderStream = IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>;
using FromClient = StreamReadMessage.Types.FromClient;
using FromServer = StreamReadMessage.Types.FromServer;

public class ReaderUnitTests
{
    private static readonly FromServer InitResponseFromServer = new()
    {
        Status = StatusIds.Types.StatusCode.Success,
        InitResponse = new StreamReadMessage.Types.InitResponse { SessionId = "SessionId" }
    };

    private readonly Mock<IDriver> _mockIDriver = new();
    private readonly Mock<ReaderStream> _mockStream = new();
    private readonly ValueTask<bool> _lastMoveNext;

    public ReaderUnitTests()
    {
        _mockIDriver.Setup(driver => driver.BidirectionalStreamCall(
            It.IsAny<Method<FromClient, FromServer>>(),
            It.IsAny<GrpcRequestSettings>())
        ).ReturnsAsync(_mockStream.Object);

        _mockIDriver.Setup(driver => driver.LoggerFactory).Returns(Utils.LoggerFactory);

        var tcsLastMoveNext = new TaskCompletionSource<bool>();

        _lastMoveNext = new ValueTask<bool>(tcsLastMoveNext.Task);
        _mockStream.Setup(stream => stream.RequestStreamComplete()).Returns(() =>
        {
            tcsLastMoveNext.TrySetResult(false);

            return Task.CompletedTask;
        });
    }

    /*
     * Performed invocations:

       Mock<IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer Tester" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer Tester" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "200" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "startPartitionSessionResponse": { "partitionSessionId": "1" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "50" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "commitOffsetRequest": { "commitOffsets": [ { "partitionSessionId": "1", "offsets": [ { "end": "1" } ] } ] } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync() [Maybe]
     */
    [Fact]
    public async Task Initialize_WhenFailWriteMessage_ShouldRetryInitializeAndReadThenCommitMessage()
    {
        var tcsMoveNext = new TaskCompletionSource<bool>();
        var tcsCommitMessage = new TaskCompletionSource<bool>();

        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .ThrowsAsync(new Driver.TransportException(new RpcException(Grpc.Core.Status.DefaultCancelled)))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsMoveNext.SetResult(true);

                return Task.CompletedTask;
            })
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsCommitMessage.SetResult(true);

                return Task.CompletedTask;
            });

        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(tcsMoveNext.Task))
            .Returns(new ValueTask<bool>(tcsCommitMessage.Task))
            .Returns(_lastMoveNext);

        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponseFromServer)
            .Returns(StartPartitionSessionRequest())
            .Returns(ReadResponse(0, BitConverter.GetBytes(100)))
            .Returns(CommitOffsetResponse());

        await using var reader = new ReaderBuilder<int>(_mockIDriver.Object)
        {
            ConsumerName = "Consumer Tester",
            MemoryUsageMaxBytes = 200,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        var message = await reader.ReadAsync();
        await message.CommitAsync();
        Assert.Equal(100, message.Data);

        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(6));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Between(4, 5, Range.Inclusive));
        _mockStream.Verify(stream => stream.Current, Times.Exactly(4));

        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
                msg.InitRequest != null &&
                msg.InitRequest.Consumer == "Consumer Tester" &&
                msg.InitRequest.TopicsReadSettings[0].Path == "/topic")),
            Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null &&
            msg.ReadRequest.BytesSize == 200)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.StartPartitionSessionResponse != null &&
            msg.StartPartitionSessionResponse.PartitionSessionId == 1)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null &&
            msg.ReadRequest.BytesSize == 50)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.CommitOffsetRequest != null &&
            msg.CommitOffsetRequest.CommitOffsets[0].PartitionSessionId == 1 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].End == 1)));
    }

    /*
     * Performed invocations:

       Mock<IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer Tester" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer Tester" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "1000" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "startPartitionSessionResponse": { "partitionSessionId": "1" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "commitOffsetRequest": { "commitOffsets": [ { "partitionSessionId": "1", "offsets": [ { "start": "20", "end": "21" } ] } ] } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync() [Maybe]
     */
    [Fact]
    public async Task Initialize_WhenFailMoveNextAsync_ShouldRetryInitializeAndReadThenCommitMessage()
    {
        var tcsMoveNext = new TaskCompletionSource<bool>();
        var tcsCommitMessage = new TaskCompletionSource<bool>();

        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsMoveNext.SetResult(true);

                return Task.CompletedTask;
            })
            .Returns(() =>
            {
                tcsCommitMessage.SetResult(true);

                return Task.CompletedTask;
            });

        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ThrowsAsync(new Driver.TransportException(new RpcException(Grpc.Core.Status.DefaultCancelled)))
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(tcsMoveNext.Task))
            .Returns(new ValueTask<bool>(tcsCommitMessage.Task))
            .Returns(_lastMoveNext);

        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponseFromServer)
            .Returns(StartPartitionSessionRequest(20))
            .Returns(ReadResponse(20, Encoding.UTF8.GetBytes("Hello World!")))
            .Returns(CommitOffsetResponse(21));

        await using var reader = new ReaderBuilder<string>(_mockIDriver.Object)
        {
            ConsumerName = "Consumer Tester 2",
            MemoryUsageMaxBytes = 1000,
            SubscribeSettings = { new SubscribeSettings("/topic-new") }
        }.Build();

        var message = await reader.ReadAsync();
        await message.CommitAsync();
        Assert.Equal("Hello World!", message.Data);

        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(5));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Between(5, 6, Range.Inclusive));
        _mockStream.Verify(stream => stream.Current, Times.Exactly(4));

        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
                msg.InitRequest != null &&
                msg.InitRequest.Consumer == "Consumer Tester 2" &&
                msg.InitRequest.TopicsReadSettings[0].Path == "/topic-new")),
            Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null &&
            msg.ReadRequest.BytesSize == 1000)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.StartPartitionSessionResponse != null &&
            msg.StartPartitionSessionResponse.PartitionSessionId == 1)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null &&
            msg.ReadRequest.BytesSize == 50)), Times.Never);
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.CommitOffsetRequest != null &&
            msg.CommitOffsetRequest.CommitOffsets[0].PartitionSessionId == 1 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].Start == 20 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].End == 21)));
    }

    /*
     * Performed invocations:

       Mock<IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "100" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "startPartitionSessionResponse": { "partitionSessionId": "1" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "25" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "25" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "commitOffsetRequest": { "commitOffsets": [ { "partitionSessionId": "1", "offsets": [ { "start": "10", "end": "12" } ] } ] } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync() [Maybe]
     */
    [Fact]
    public async Task Initialize_WhenInitResponseStatusIsRetryable_ShouldRetryInitializeAndReadBatchThenCommitMessages()
    {
        var tcsMoveNext = new TaskCompletionSource<bool>();
        var tcsCommitMessage = new TaskCompletionSource<bool>();

        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsMoveNext.SetResult(true);

                return Task.CompletedTask;
            })
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsCommitMessage.SetResult(true);

                return Task.CompletedTask;
            });

        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(tcsMoveNext.Task))
            .Returns(new ValueTask<bool>(tcsCommitMessage.Task))
            .Returns(_lastMoveNext);

        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(new FromServer
            {
                Status = StatusIds.Types.StatusCode.BadSession,
                Issues = { new IssueMessage { Message = "Some message" } }
            })
            .Returns(InitResponseFromServer)
            .Returns(StartPartitionSessionRequest(10))
            .Returns(ReadResponse(10, Encoding.UTF8.GetBytes("First"), Encoding.UTF8.GetBytes("Second")))
            .Returns(CommitOffsetResponse(12));

        await using var reader = new ReaderBuilder<string>(_mockIDriver.Object)
        {
            ConsumerName = "Consumer",
            MemoryUsageMaxBytes = 100,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        var message = await reader.ReadBatchAsync();
        await message.CommitBatchAsync();
        Assert.Equal(2, message.Batch.Count);
        Assert.Equal("First", message.Batch[0].Data);
        Assert.Equal("Second", message.Batch[1].Data);

        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(7));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Between(5, 6, Range.Inclusive));
        _mockStream.Verify(stream => stream.Current, Times.Exactly(5));

        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
                msg.InitRequest != null &&
                msg.InitRequest.Consumer == "Consumer" &&
                msg.InitRequest.TopicsReadSettings[0].Path == "/topic")),
            Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null &&
            msg.ReadRequest.BytesSize == 100)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.StartPartitionSessionResponse != null &&
            msg.StartPartitionSessionResponse.PartitionSessionId == 1)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null &&
            msg.ReadRequest.BytesSize == 25)), Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.CommitOffsetRequest != null &&
            msg.CommitOffsetRequest.CommitOffsets[0].PartitionSessionId == 1 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].Start == 10 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].End == 12)));
    }

    /*
     * Performed invocations:

       Mock<IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
     */
    [Fact]
    public async Task Initialize_WhenInitResponseStatusIsNotRetryable_ShouldThrowReaderExceptionOnRead()
    {
        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask);

        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true);

        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(new FromServer
            {
                Status = StatusIds.Types.StatusCode.SchemeError,
                Issues = { new IssueMessage { Message = "Topic not found" } }
            });

        await using var reader = new ReaderBuilder<string>(_mockIDriver.Object)
        {
            ConsumerName = "Consumer",
            MemoryUsageMaxBytes = 100,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        Assert.Equal("Initialization failed: Status: SchemeError, Issues:\n[0] Fatal: Topic not found\n",
            (await Assert.ThrowsAsync<ReaderException>(async () => await reader.ReadAsync())).Message);
        Assert.Equal("Initialization failed: Status: SchemeError, Issues:\n[0] Fatal: Topic not found\n",
            (await Assert.ThrowsAsync<ReaderException>(async () => await reader.ReadBatchAsync())).Message);

        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.InitRequest != null &&
            msg.InitRequest.Consumer == "Consumer" &&
            msg.InitRequest.TopicsReadSettings[0].Path == "/topic")));
        _mockStream.Verify(stream => stream.MoveNextAsync());
        _mockStream.Verify(stream => stream.Current);
        _mockStream.VerifyNoOtherCalls();
    }

    /*
     * Performed invocations:

       Mock<IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "200" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "200" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "startPartitionSessionResponse": { "partitionSessionId": "1" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "50" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "commitOffsetRequest": { "commitOffsets": [ { "partitionSessionId": "1", "offsets": [ { "end": "3" } ] } ] } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync() [Maybe]
     */
    [Fact]
    public async Task
        RunProcessingTopic_WhenMoveNextAsyncStartPartitionSessionRequestThrowTransportException_ShouldRetryInitializeAndReadBatchThenCommitMessages()
    {
        var tcsMoveNext = new TaskCompletionSource<bool>();
        var tcsCommitMessage = new TaskCompletionSource<bool>();

        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsMoveNext.SetResult(true);

                return Task.CompletedTask;
            })
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsCommitMessage.SetResult(true);

                return Task.CompletedTask;
            });

        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ThrowsAsync(new Driver.TransportException(new RpcException(Grpc.Core.Status.DefaultCancelled)))
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(tcsMoveNext.Task))
            .Returns(new ValueTask<bool>(tcsCommitMessage.Task))
            .Returns(_lastMoveNext);

        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponseFromServer)
            .Returns(InitResponseFromServer)
            .Returns(StartPartitionSessionRequest())
            .Returns(ReadResponse(0, BitConverter.GetBytes(0L), BitConverter.GetBytes(1L), BitConverter.GetBytes(2L)))
            .Returns(CommitOffsetResponse(3));

        await using var reader = new ReaderBuilder<long>(_mockIDriver.Object)
        {
            ConsumerName = "Consumer",
            MemoryUsageMaxBytes = 200,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        var message = await reader.ReadBatchAsync();
        await message.CommitBatchAsync();
        Assert.Equal(3, message.Batch.Count);
        for (var i = 0; i < 3; i++)
        {
            var m = message.Batch[i];
            Assert.Equal(i, m.Data);
            Assert.Equal("ProducerId", m.ProducerId);
            Assert.Equal(1, m.PartitionId);
            Assert.Equal("/topic", m.Topic);
        }

        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(7));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Between(6, 7, Range.Inclusive));
        _mockStream.Verify(stream => stream.Current, Times.Exactly(5));

        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
                msg.InitRequest != null &&
                msg.InitRequest.Consumer == "Consumer" &&
                msg.InitRequest.TopicsReadSettings[0].Path == "/topic")),
            Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null &&
            msg.ReadRequest.BytesSize == 200)), Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.StartPartitionSessionResponse != null &&
            msg.StartPartitionSessionResponse.PartitionSessionId == 1)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null &&
            msg.ReadRequest.BytesSize == 50)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.CommitOffsetRequest != null &&
            msg.CommitOffsetRequest.CommitOffsets[0].PartitionSessionId == 1 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].Start == 0 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].End == 3)));
    }

    /*
     * Performed invocations:

       Mock<IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "250" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "startPartitionSessionResponse": { "partitionSessionId": "1" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "250" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "startPartitionSessionResponse": { "partitionSessionId": "1" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "commitOffsetRequest": { "commitOffsets": [ { "partitionSessionId": "1", "offsets": [ { "start": "100", "end": "101" } ] } ] } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "commitOffsetRequest": { "commitOffsets": [ { "partitionSessionId": "1", "offsets": [ { "start": "101", "end": "102" } ] } ] } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "50" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "commitOffsetRequest": { "commitOffsets": [ { "partitionSessionId": "1", "offsets": [ { "start": "102", "end": "103" } ] } ] } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync() [Maybe]
     */
    [Fact]
    public async Task
        RunProcessingTopic_WhenStartPartitionSessionResponseThrowTransportException_ShouldRetryInitializeAndReadsThenCommitMessages()
    {
        var tcsMoveNextFirst = new TaskCompletionSource<bool>();
        var tcsMoveNextSecond = new TaskCompletionSource<bool>();
        var tcsCommitMessage1 = new TaskCompletionSource<bool>();
        var tcsCommitMessage2 = new TaskCompletionSource<bool>();
        var tcsCommitMessage3 = new TaskCompletionSource<bool>();

        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Throws(() =>
            {
                tcsMoveNextFirst.SetResult(false);

                return new Driver.TransportException(new RpcException(Grpc.Core.Status.DefaultCancelled));
            })
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsMoveNextSecond.SetResult(true);

                return Task.CompletedTask;
            })
            .Returns(() =>
            {
                tcsCommitMessage1.SetResult(true);

                return Task.CompletedTask;
            })
            .Returns(() =>
            {
                tcsCommitMessage2.SetResult(true);

                return Task.CompletedTask;
            })
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsCommitMessage3.SetResult(true);

                return Task.CompletedTask;
            });

        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(tcsMoveNextFirst.Task))
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(tcsMoveNextSecond.Task))
            .Returns(new ValueTask<bool>(tcsCommitMessage1.Task))
            .Returns(new ValueTask<bool>(tcsCommitMessage2.Task))
            .Returns(new ValueTask<bool>(tcsCommitMessage3.Task))
            .Returns(_lastMoveNext);

        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponseFromServer)
            .Returns(StartPartitionSessionRequest(100))
            .Returns(InitResponseFromServer)
            .Returns(StartPartitionSessionRequest(100))
            .Returns(ReadResponse(100, BitConverter.GetBytes(1L), BitConverter.GetBytes(2L), BitConverter.GetBytes(3L)))
            .Returns(CommitOffsetResponse(101))
            .Returns(CommitOffsetResponse(102))
            .Returns(CommitOffsetResponse(103));

        await using var reader = new ReaderBuilder<long>(_mockIDriver.Object)
        {
            ConsumerName = "Consumer",
            MemoryUsageMaxBytes = 250,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        var message1 = await reader.ReadAsync();
        await message1.CommitAsync();
        Assert.Equal(1, message1.Data);

        var message2 = await reader.ReadAsync();
        await message2.CommitAsync();
        Assert.Equal(2, message2.Data);

        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null &&
            msg.ReadRequest.BytesSize == 50)), Times.Never);

        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.CommitOffsetRequest != null &&
            msg.CommitOffsetRequest.CommitOffsets[0].PartitionSessionId == 1 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].Start == 100 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].End == 101)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.CommitOffsetRequest != null &&
            msg.CommitOffsetRequest.CommitOffsets[0].PartitionSessionId == 1 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].Start == 101 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].End == 102)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.CommitOffsetRequest != null &&
            msg.CommitOffsetRequest.CommitOffsets[0].PartitionSessionId == 1 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].Start == 102 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].End == 103)), Times.Never);

        var message3 = await reader.ReadAsync();
        await message3.CommitAsync();
        Assert.Equal(3, message3.Data);

        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(10));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Between(9, 10, Range.Inclusive));
        _mockStream.Verify(stream => stream.Current, Times.Exactly(8));

        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
                msg.InitRequest != null &&
                msg.InitRequest.Consumer == "Consumer" &&
                msg.InitRequest.TopicsReadSettings[0].Path == "/topic")),
            Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null &&
            msg.ReadRequest.BytesSize == 250)), Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.StartPartitionSessionResponse != null &&
            msg.StartPartitionSessionResponse.PartitionSessionId == 1)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null &&
            msg.ReadRequest.BytesSize == 50)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.CommitOffsetRequest != null &&
            msg.CommitOffsetRequest.CommitOffsets[0].PartitionSessionId == 1 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].Start == 102 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].End == 103)));
    }

    /*
     *    Mock<IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "100" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "startPartitionSessionResponse": { "partitionSessionId": "1" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "50" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "100" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "startPartitionSessionResponse": { "partitionSessionId": "1" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "50" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "commitOffsetRequest": { "commitOffsets": [ { "partitionSessionId": "1", "offsets": [ { "end": "101" } ] } ] } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync() [Maybe]
     */
    [Fact]
    public async Task
        RunProcessingTopic_WhenReadRequestAfterInitializeThrowTransportException_ShouldRetryInitializeAndReadThenCommitMessage()
    {
        var tcsMoveNextFirst = new TaskCompletionSource<bool>();
        var tcsMoveNextSecond = new TaskCompletionSource<bool>();
        var tcsMoveNextThird = new TaskCompletionSource<bool>();
        var tcsCommitMessage = new TaskCompletionSource<bool>();

        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsMoveNextFirst.SetResult(true);

                return Task.CompletedTask;
            })
            .Throws(() =>
            {
                var error = new Driver.TransportException(new RpcException(Grpc.Core.Status.DefaultCancelled));
                tcsMoveNextSecond.TrySetException(error);

                return error;
            })
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsMoveNextThird.SetResult(true);

                return Task.CompletedTask;
            })
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsCommitMessage.SetResult(true);

                return Task.CompletedTask;
            });

        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(tcsMoveNextFirst.Task))
            .Returns(new ValueTask<bool>(tcsMoveNextSecond.Task))
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(tcsMoveNextThird.Task))
            .Returns(new ValueTask<bool>(tcsCommitMessage.Task))
            .Returns(_lastMoveNext);

        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponseFromServer)
            .Returns(StartPartitionSessionRequest())
            .Returns(ReadResponse(100, BitConverter.GetBytes(100L)))
            .Returns(InitResponseFromServer)
            .Returns(StartPartitionSessionRequest())
            .Returns(ReadResponse(100, BitConverter.GetBytes(100L)))
            .Returns(CommitOffsetResponse(101));

        await using var reader = new ReaderBuilder<long>(_mockIDriver.Object)
        {
            ConsumerName = "Consumer",
            MemoryUsageMaxBytes = 100,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        var message = await reader.ReadAsync();
        Assert.Equal("ReaderSession[SessionId] was deactivated",
            (await Assert.ThrowsAsync<ReaderException>(() => message.CommitAsync())).Message);
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.CommitOffsetRequest != null)), Times.Never());
        Assert.Equal(100, message.Data);

        message = await reader.ReadAsync();
        await message.CommitAsync();
        Assert.Equal(100, message.Data);

        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(9));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Between(8, 9, Range.Inclusive));
        _mockStream.Verify(stream => stream.Current, Times.Exactly(7));

        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.InitRequest != null &&
            msg.InitRequest.Consumer == "Consumer" &&
            msg.InitRequest.TopicsReadSettings[0].Path == "/topic")), Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null && msg.ReadRequest.BytesSize == 100)), Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.StartPartitionSessionResponse != null &&
            msg.StartPartitionSessionResponse.PartitionSessionId == 1)), Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null && msg.ReadRequest.BytesSize == 100)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.CommitOffsetRequest != null &&
            msg.CommitOffsetRequest.CommitOffsets[0].PartitionSessionId == 1 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].Start == 0 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].End == 101)));
    }

    /*
     * Performed invocations:

       Mock<IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "100" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "startPartitionSessionResponse": { "partitionSessionId": "1" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "25" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "25" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "100" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "startPartitionSessionResponse": { "partitionSessionId": "1" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "commitOffsetRequest": { "commitOffsets": [ { "partitionSessionId": "1", "offsets": [ { "end": "102" } ] } ] } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "25" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "25" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync() [Maybe]
     */
    [Fact]
    public async Task
        RunProcessingTopic_WhenCommitThrowTransportException_ShouldRetryInitializeAndReadBatchThenCommitMessages()
    {
        var tcsMoveNextFirst = new TaskCompletionSource<bool>();
        var tcsMoveNextSecond = new TaskCompletionSource<bool>();
        var tcsMoveNextThird = new TaskCompletionSource<bool>();
        var tcsCommitMessage = new TaskCompletionSource<bool>();

        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsMoveNextFirst.SetResult(true);

                return Task.CompletedTask;
            })
            .Returns(Task.CompletedTask)
            .Throws(() =>
            {
                var error = new Driver.TransportException(new RpcException(Grpc.Core.Status.DefaultCancelled));
                tcsMoveNextSecond.TrySetException(error);

                return error;
            })
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsMoveNextThird.SetResult(true);

                return Task.CompletedTask;
            })
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsCommitMessage.SetResult(true);

                return Task.CompletedTask;
            });

        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(tcsMoveNextFirst.Task))
            .Returns(new ValueTask<bool>(tcsMoveNextSecond.Task))
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(tcsMoveNextThird.Task))
            .Returns(new ValueTask<bool>(tcsCommitMessage.Task))
            .Returns(_lastMoveNext);

        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponseFromServer)
            .Returns(StartPartitionSessionRequest())
            .Returns(ReadResponse(100, Encoding.UTF8.GetBytes("Hello"), Encoding.UTF8.GetBytes("World!")))
            .Returns(InitResponseFromServer)
            .Returns(StartPartitionSessionRequest())
            .Returns(ReadResponse(100, Encoding.UTF8.GetBytes("Hello"), Encoding.UTF8.GetBytes("World!")))
            .Returns(CommitOffsetResponse(102));

        await using var reader = new ReaderBuilder<string>(_mockIDriver.Object)
        {
            ConsumerName = "Consumer",
            MemoryUsageMaxBytes = 100,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        var batch = await reader.ReadBatchAsync();
        Assert.Equal("ReaderSession[SessionId] was deactivated",
            (await Assert.ThrowsAsync<ReaderException>(() => batch.CommitBatchAsync())).Message);
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.CommitOffsetRequest != null)), Times.Never());
        Assert.Equal("Hello", batch.Batch[0].Data);
        Assert.Equal("World!", batch.Batch[1].Data);

        batch = await reader.ReadBatchAsync();
        await batch.CommitBatchAsync();
        Assert.Equal("Hello", batch.Batch[0].Data);
        Assert.Equal("World!", batch.Batch[1].Data);

        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(11));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Between(8, 9, Range.Inclusive));
        _mockStream.Verify(stream => stream.Current, Times.Exactly(7));

        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.InitRequest != null &&
            msg.InitRequest.Consumer == "Consumer" &&
            msg.InitRequest.TopicsReadSettings[0].Path == "/topic")), Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null && msg.ReadRequest.BytesSize == 100)), Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.StartPartitionSessionResponse != null &&
            msg.StartPartitionSessionResponse.PartitionSessionId == 1)), Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null && msg.ReadRequest.BytesSize == 100)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.CommitOffsetRequest != null &&
            msg.CommitOffsetRequest.CommitOffsets[0].PartitionSessionId == 1 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].Start == 0 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].End == 102)));
    }

    /*
     * Performed invocations:

       Mock<IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "100" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "startPartitionSessionResponse": { "partitionSessionId": "1" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "25" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "25" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "100" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "startPartitionSessionResponse": { "partitionSessionId": "1" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "commitOffsetRequest": { "commitOffsets": [ { "partitionSessionId": "1", "offsets": [ { "end": "102" } ] } ] } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "25" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "25" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync() [Maybe]
     */
    [Fact]
    public async Task
        RunProcessingTopic_WhenNextMoveAsyncThrowTransportExceptionBeforeCommitThenSkipGapsOnNextRanges_ShouldRetryInitializeAndReadThenCommitMessages()
    {
        var readResponseWithGaps = new FromServer
        {
            Status = StatusIds.Types.StatusCode.Success,
            ReadResponse = new StreamReadMessage.Types.ReadResponse
            {
                BytesSize = 50, PartitionData =
                {
                    new StreamReadMessage.Types.ReadResponse.Types.PartitionData
                    {
                        PartitionSessionId = 1,
                        Batches =
                        {
                            new StreamReadMessage.Types.ReadResponse.Types.Batch
                            {
                                ProducerId = "ProducerId",
                                MessageData =
                                {
                                    new StreamReadMessage.Types.ReadResponse.Types.MessageData
                                    {
                                        Data = ByteString.CopyFrom(Encoding.UTF8.GetBytes("Hello")),
                                        Offset = 10,
                                        CreatedAt = new Timestamp()
                                    },

                                    new StreamReadMessage.Types.ReadResponse.Types.MessageData
                                    {
                                        Data = ByteString.CopyFrom(Encoding.UTF8.GetBytes("World!")),
                                        Offset = 20,
                                        CreatedAt = new Timestamp()
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        var tcsMoveNextFirst = new TaskCompletionSource<bool>();
        var tcsMoveNextSecond = new TaskCompletionSource<bool>();
        var tcsMoveNextThird = new TaskCompletionSource<bool>();
        var tcsCommitMessage = new TaskCompletionSource<bool>();

        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsMoveNextFirst.SetResult(true);

                return Task.CompletedTask;
            })
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsMoveNextSecond.TrySetException(
                    new Driver.TransportException(new RpcException(Grpc.Core.Status.DefaultCancelled)));

                return Task.CompletedTask;
            })
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsMoveNextThird.SetResult(true);

                return Task.CompletedTask;
            })
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsCommitMessage.SetResult(true);

                return Task.CompletedTask;
            });

        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(tcsMoveNextFirst.Task))
            .Returns(new ValueTask<bool>(tcsMoveNextSecond.Task))
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(tcsMoveNextThird.Task))
            .Returns(new ValueTask<bool>(tcsCommitMessage.Task))
            .Returns(_lastMoveNext);

        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponseFromServer)
            .Returns(StartPartitionSessionRequest())
            .Returns(readResponseWithGaps)
            .Returns(InitResponseFromServer)
            .Returns(StartPartitionSessionRequest())
            .Returns(readResponseWithGaps)
            .Returns(CommitOffsetResponse(102));

        await using var reader = new ReaderBuilder<string>(_mockIDriver.Object)
        {
            ConsumerName = "Consumer",
            MemoryUsageMaxBytes = 100,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        var batch = await reader.ReadBatchAsync();
        Assert.Equal("ReaderSession[SessionId] was deactivated",
            (await Assert.ThrowsAsync<ReaderException>(() => batch.CommitBatchAsync())).Message);
        Assert.Equal("Hello", batch.Batch[0].Data);
        Assert.Equal("World!", batch.Batch[1].Data);

        batch = await reader.ReadBatchAsync();
        await batch.CommitBatchAsync();
        Assert.Equal("Hello", batch.Batch[0].Data);
        Assert.Equal("World!", batch.Batch[1].Data);

        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.AtLeast(11));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Between(8, 9, Range.Inclusive));
        _mockStream.Verify(stream => stream.Current, Times.Exactly(7));

        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.InitRequest != null && msg.InitRequest.Consumer == "Consumer" &&
            msg.InitRequest.TopicsReadSettings[0].Path == "/topic")), Times.Between(2, 3, Range.Inclusive));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null && msg.ReadRequest.BytesSize == 100)), Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.StartPartitionSessionResponse != null &&
            msg.StartPartitionSessionResponse.PartitionSessionId == 1)), Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null && msg.ReadRequest.BytesSize == 25)), Times.Exactly(4));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.CommitOffsetRequest != null &&
            msg.CommitOffsetRequest.CommitOffsets[0].PartitionSessionId == 1 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].Start == 0 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].End == 21)), Times.AtLeast(1));
    }

    /*
     *
       Performed invocations:

          Mock<IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>:1> (stream):

             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer" } })
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "1000" } })
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "startPartitionSessionResponse": { "partitionSessionId": "1" } })
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "commitOffsetRequest": { "commitOffsets": [ { "partitionSessionId": "1", "offsets": [ { "end": "23" } ] } ] } })
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer" } })
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "1000" } })
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "startPartitionSessionResponse": { "partitionSessionId": "1" } })
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "commitOffsetRequest": { "commitOffsets": [ { "partitionSessionId": "1", "offsets": [ { "end": "23" } ] } ] } })
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
             IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync() [Maybe]
     */
    [Fact]
    public async Task
        RunProcessingTopic_WhenMoveNextAsyncThrowTransportExceptionAfterCommit_ShouldRetryInitializeAndReadThenCommitMessages()
    {
        var bytes = new byte[] { 0, 1, 2, 3 };
        var tcsMoveNextFirst = new TaskCompletionSource<bool>();
        var tcsMoveNextSecond = new TaskCompletionSource<bool>();
        var tcsMoveNextThird = new TaskCompletionSource<bool>();
        var tcsCommitMessage = new TaskCompletionSource<bool>();

        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsMoveNextFirst.SetResult(true);

                return Task.CompletedTask;
            })
            .Returns(() =>
            {
                tcsMoveNextSecond.TrySetException(
                    new Driver.TransportException(new RpcException(Grpc.Core.Status.DefaultCancelled)));

                return Task.CompletedTask;
            })
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsMoveNextThird.SetResult(true);

                return Task.CompletedTask;
            })
            .Returns(() =>
            {
                tcsCommitMessage.SetResult(true);

                return Task.CompletedTask;
            });

        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(tcsMoveNextFirst.Task))
            .Returns(new ValueTask<bool>(tcsMoveNextSecond.Task))
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(tcsMoveNextThird.Task))
            .Returns(new ValueTask<bool>(tcsCommitMessage.Task))
            .ReturnsAsync(true)
            .Returns(_lastMoveNext);

        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponseFromServer)
            .Returns(StartPartitionSessionRequest())
            .Returns(ReadResponse(20, bytes, bytes, bytes))
            .Returns(InitResponseFromServer)
            .Returns(StartPartitionSessionRequest())
            .Returns(ReadResponse(20, bytes, bytes, bytes))
            .Returns(CommitOffsetResponse(10))
            .Returns(CommitOffsetResponse(23));

        await using var reader = new ReaderBuilder<byte[]>(_mockIDriver.Object)
        {
            ConsumerName = "Consumer",
            MemoryUsageMaxBytes = 1000,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        var batch = await reader.ReadBatchAsync();
        Assert.Equal("ReaderSession[SessionId] was deactivated",
            (await Assert.ThrowsAsync<ReaderException>(() => batch.CommitBatchAsync())).Message);
        Assert.Equal(3, batch.Batch.Count);
        foreach (var message in batch.Batch)
        {
            Assert.Equal(bytes, message.Data);
        }

        batch = await reader.ReadBatchAsync();
        await batch.CommitBatchAsync();
        Assert.Equal(3, batch.Batch.Count);
        foreach (var message in batch.Batch)
        {
            Assert.Equal(bytes, message.Data);
        }

        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(8));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Between(9, 10, Range.Inclusive));
        _mockStream.Verify(stream => stream.Current, Times.Exactly(8));

        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.InitRequest != null &&
            msg.InitRequest.Consumer == "Consumer" &&
            msg.InitRequest.TopicsReadSettings[0].Path == "/topic")), Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null && msg.ReadRequest.BytesSize == 1000)), Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null)), Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.StartPartitionSessionResponse != null &&
            msg.StartPartitionSessionResponse.PartitionSessionId == 1)), Times.Exactly(2));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.CommitOffsetRequest != null &&
            msg.CommitOffsetRequest.CommitOffsets[0].PartitionSessionId == 1 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].Start == 0 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].End == 23)));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task
        RunProcessingTopic_WhenStopPartitionSessionRequestBeforeCommit_ThrowReaderExceptionOnCommit(bool graceful)
    {
        var tcsMoveNext = new TaskCompletionSource<bool>();
        var stopPartitionSessionRequest = new TaskCompletionSource<bool>();

        var sequentialResultWrite = _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsMoveNext.SetResult(true);

                return Task.CompletedTask;
            })
            .Returns(() =>
            {
                stopPartitionSessionRequest.SetResult(true);

                return Task.CompletedTask;
            });

        TaskCompletionSource waitStopPartitionSessionResponse;
        if (graceful)
        {
            waitStopPartitionSessionResponse = new TaskCompletionSource();
            sequentialResultWrite.Returns(() =>
            {
                waitStopPartitionSessionResponse.SetResult();
                return Task.CompletedTask;
            });
        }
        else
        {
            waitStopPartitionSessionResponse = new TaskCompletionSource();
            waitStopPartitionSessionResponse.SetResult();
        }

        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(tcsMoveNext.Task))
            .Returns(new ValueTask<bool>(stopPartitionSessionRequest.Task))
            .ReturnsAsync(true)
            .Returns(_lastMoveNext);

        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponseFromServer)
            .Returns(StartPartitionSessionRequest())
            .Returns(ReadResponse(0, BitConverter.GetBytes(100), BitConverter.GetBytes(100)))
            .Returns(new FromServer
            {
                Status = StatusIds.Types.StatusCode.Success,
                StopPartitionSessionRequest = new StreamReadMessage.Types.StopPartitionSessionRequest
                {
                    PartitionSessionId = 1,
                    CommittedOffset = 1,
                    Graceful = graceful
                }
            })
            .Returns(CommitOffsetResponse(2));

        await using var reader = new ReaderBuilder<int>(_mockIDriver.Object)
        {
            ConsumerName = "Consumer",
            MemoryUsageMaxBytes = 1000,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        var firstMessage = await reader.ReadAsync();
        var secondMessage = await reader.ReadAsync();

        Assert.Equal(100, firstMessage.Data);
        await firstMessage.CommitAsync();
        Assert.Equal(100, secondMessage.Data);

        Assert.Equal("PartitionSession[1] was closed by server.",
            (await Assert.ThrowsAsync<ReaderException>(() => secondMessage.CommitAsync())).Message);

        await waitStopPartitionSessionResponse.Task;

        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(4 + (graceful ? 1 : 0)));
        _mockStream.Verify(stream => stream.MoveNextAsync(),
            Times.Between(4, 6, Range.Inclusive));
        _mockStream.Verify(stream => stream.Current, Times.Between(4, 5, Range.Inclusive));

        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.InitRequest != null &&
            msg.InitRequest.Consumer == "Consumer" &&
            msg.InitRequest.TopicsReadSettings[0].Path == "/topic")));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null && msg.ReadRequest.BytesSize == 1000)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.StartPartitionSessionResponse != null &&
            msg.StartPartitionSessionResponse.PartitionSessionId == 1)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.CommitOffsetRequest != null &&
            msg.CommitOffsetRequest.CommitOffsets[0].PartitionSessionId == 1 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].Start == 0 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].End == 1)));

        if (graceful)
        {
            _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
                msg.StopPartitionSessionResponse != null &&
                msg.StopPartitionSessionResponse.PartitionSessionId == 1)));
        }
    }

    [Fact]
    public async Task ReadAsync_WhenFailDeserializer_ThrowReaderExceptionAndInvokeReadRequest()
    {
        var tcsMoveNext = new TaskCompletionSource<bool>();

        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .ThrowsAsync(new Driver.TransportException(new RpcException(Grpc.Core.Status.DefaultCancelled)))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsMoveNext.SetResult(true);

                return Task.CompletedTask;
            })
            .Returns(Task.CompletedTask);

        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(tcsMoveNext.Task))
            .Returns(_lastMoveNext);

        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponseFromServer)
            .Returns(StartPartitionSessionRequest())
            .Returns(ReadResponse(0, BitConverter.GetBytes(100)));

        await using var reader = new ReaderBuilder<int>(_mockIDriver.Object)
        {
            ConsumerName = "Consumer",
            MemoryUsageMaxBytes = 100,
            SubscribeSettings = { new SubscribeSettings("/topic") },
            Deserializer = new FailDeserializer()
        }.Build();

        Assert.Equal("Error when deserializing message data",
            (await Assert.ThrowsAsync<ReaderException>(() => reader.ReadAsync().AsTask())).Message);
    }

    /*
     *
       Performed invocations:

       Mock<IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "initRequest": { "topicsReadSettings": [ { "path": "/topic" } ], "consumer": "Consumer Tester" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "readRequest": { "bytesSize": "1000" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.AuthToken
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.AuthToken
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "startPartitionSessionResponse": { "partitionSessionId": "1" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.AuthToken
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "updateTokenRequest": { "token": "Token2" } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Write({ "commitOffsetRequest": { "commitOffsets": [ { "partitionSessionId": "1", "offsets": [ { "end": "1" } ] } ] } })
          IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>.Current
     */
    [Fact]
    public async Task ReadAsync_WhenTokenIsUpdatedOneTime_SuccessUpdateToken()
    {
        _mockStream.SetupSequence(stream => stream.AuthToken())
            .ReturnsAsync("Token1")
            .ReturnsAsync("Token1")
            .ReturnsAsync("Token2")
            .ReturnsAsync("Token2");

        var tcsMoveNext = new TaskCompletionSource<bool>();
        var tcsCommitMessage = new TaskCompletionSource<bool>();

        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsMoveNext.SetResult(true);

                return Task.CompletedTask;
            })
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsCommitMessage.SetResult(true);

                return Task.CompletedTask;
            });

        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(tcsMoveNext.Task))
            .Returns(new ValueTask<bool>(tcsCommitMessage.Task))
            .Returns(_lastMoveNext);

        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponseFromServer)
            .Returns(StartPartitionSessionRequest())
            .Returns(ReadResponse(0, BitConverter.GetBytes(100)))
            .Returns(CommitOffsetResponse());

        await using var reader = new ReaderBuilder<int>(_mockIDriver.Object)
        {
            ConsumerName = "Consumer Tester",
            MemoryUsageMaxBytes = 1000,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        var message = await reader.ReadAsync();
        await message.CommitAsync();
        Assert.Equal(100, message.Data);

        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(5));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Between(4, 5, Range.Inclusive));
        _mockStream.Verify(stream => stream.Current, Times.Exactly(4));

        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.InitRequest != null &&
            msg.InitRequest.Consumer == "Consumer Tester" &&
            msg.InitRequest.TopicsReadSettings[0].Path == "/topic")));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null &&
            msg.ReadRequest.BytesSize == 1000)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.StartPartitionSessionResponse != null &&
            msg.StartPartitionSessionResponse.PartitionSessionId == 1)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.ReadRequest != null)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.CommitOffsetRequest != null &&
            msg.CommitOffsetRequest.CommitOffsets[0].PartitionSessionId == 1 &&
            msg.CommitOffsetRequest.CommitOffsets[0].Offsets[0].End == 1)));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.UpdateTokenRequest != null &&
            msg.UpdateTokenRequest.Token == "Token2")));
    }

    [Fact]
    public async Task DisposeAsync_WhenCommitMessagesInFlight_CompleteThisCommits()
    {
        var tcsMoveNext = new TaskCompletionSource<bool>();
        var tcsCommitMessage = new TaskCompletionSource<bool>();

        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsMoveNext.SetResult(true);

                return Task.CompletedTask;
            })
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask);

        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(tcsMoveNext.Task))
            .Returns(new ValueTask<bool>(tcsCommitMessage.Task))
            .Returns(_lastMoveNext);

        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponseFromServer)
            .Returns(StartPartitionSessionRequest())
            .Returns(ReadResponse(0, BitConverter.GetBytes(100), BitConverter.GetBytes(1000)))
            .Returns(CommitOffsetResponse());

        var reader = new ReaderBuilder<int>(_mockIDriver.Object)
        {
            ConsumerName = "Consumer Tester",
            MemoryUsageMaxBytes = 1000,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        var message1 = await reader.ReadAsync();
        var commitTask = message1.CommitAsync();
        Assert.Equal(100, message1.Data);
        var message2 = await reader.ReadAsync();
        Assert.Equal(1000, message2.Data);
        var disposeTask = reader.DisposeAsync();
        Assert.Equal("Reader is disposed",
            (await Assert.ThrowsAsync<ReaderException>(async () => await reader.ReadAsync())).Message
        );
        Assert.Equal("Reader is disposed",
            (await Assert.ThrowsAsync<ReaderException>(async () => await reader.ReadBatchAsync())).Message
        );
        Assert.Equal("Reader is disposed",
            (await Assert.ThrowsAsync<ReaderException>(async () => await message2.CommitAsync())).Message
        );

        Assert.False(commitTask.IsCompleted);
        Assert.False(disposeTask.IsCompleted);
        tcsCommitMessage.SetResult(true);
        await commitTask;
        await disposeTask;

        Assert.Equal("Reader is disposed",
            (await Assert.ThrowsAsync<ReaderException>(async () => await reader.ReadAsync())).Message
        );
        Assert.Equal("Reader is disposed",
            (await Assert.ThrowsAsync<ReaderException>(async () => await reader.ReadBatchAsync())).Message
        );
        Assert.Equal("Reader is disposed",
            (await Assert.ThrowsAsync<ReaderException>(async () => await message2.CommitAsync())).Message
        );

        // idempotent
        await reader.DisposeAsync();
    }

    private class FailDeserializer : IDeserializer<int>
    {
        public int Deserialize(byte[] data) => throw new Exception("Some serialize exception");
    }

    private static FromServer StartPartitionSessionRequest(int commitedOffset = 0) => new()
    {
        Status = StatusIds.Types.StatusCode.Success,
        StartPartitionSessionRequest = new StreamReadMessage.Types.StartPartitionSessionRequest
        {
            CommittedOffset = commitedOffset,
            PartitionOffsets = new OffsetsRange { Start = commitedOffset, End = commitedOffset + 1000 },
            PartitionSession = new StreamReadMessage.Types.PartitionSession
                { Path = "/topic", PartitionId = 1, PartitionSessionId = 1 }
        }
    };

    private static FromServer ReadResponse(int commitedOffset = 0, params byte[][] args)
    {
        var batch = new StreamReadMessage.Types.ReadResponse.Types.Batch
        {
            ProducerId = "ProducerId"
        };

        foreach (var arg in args)
        {
            batch.MessageData.Add(
                new StreamReadMessage.Types.ReadResponse.Types.MessageData
                {
                    Data = ByteString.CopyFrom(arg),
                    Offset = commitedOffset++,
                    CreatedAt = new Timestamp()
                }
            );
        }

        return new FromServer
        {
            Status = StatusIds.Types.StatusCode.Success,
            ReadResponse = new StreamReadMessage.Types.ReadResponse
            {
                BytesSize = 50, PartitionData =
                {
                    new StreamReadMessage.Types.ReadResponse.Types.PartitionData
                    {
                        PartitionSessionId = 1,
                        Batches = { batch }
                    }
                }
            }
        };
    }

    private static FromServer CommitOffsetResponse(int committedOffset = 1) => new()
    {
        Status = StatusIds.Types.StatusCode.Success,
        CommitOffsetResponse = new StreamReadMessage.Types.CommitOffsetResponse
        {
            PartitionsCommittedOffsets =
            {
                new StreamReadMessage.Types.CommitOffsetResponse.Types.PartitionCommittedOffset
                    { PartitionSessionId = 1, CommittedOffset = committedOffset }
            }
        }
    };
}
