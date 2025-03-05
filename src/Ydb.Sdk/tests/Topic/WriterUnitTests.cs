using Grpc.Core;
using Moq;
using Moq.Language;
using Xunit;
using Ydb.Issue;
using Ydb.Sdk.Services.Topic;
using Ydb.Sdk.Services.Topic.Writer;
using Ydb.Topic;
using Codec = Ydb.Sdk.Services.Topic.Codec;
using Range = Moq.Range;

namespace Ydb.Sdk.Tests.Topic;

using WriterStream = IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>;
using FromClient = StreamWriteMessage.Types.FromClient;

public class WriterUnitTests
{
    private readonly Mock<IDriver> _mockIDriver = new();
    private readonly Mock<WriterStream> _mockStream = new();
    private readonly ValueTask<bool> _lastMoveNext;

    public WriterUnitTests()
    {
        _mockIDriver.Setup(driver => driver.BidirectionalStreamCall(
            It.IsAny<Method<FromClient, StreamWriteMessage.Types.FromServer>>(),
            It.IsAny<GrpcRequestSettings>())
        ).ReturnsAsync(_mockStream.Object);

        _mockIDriver.Setup(driver => driver.LoggerFactory).Returns(Utils.GetLoggerFactory);

        var tcsLastMoveNext = new TaskCompletionSource<bool>();

        _lastMoveNext = new ValueTask<bool>(tcsLastMoveNext.Task);
        _mockStream.Setup(stream => stream.RequestStreamComplete()).Returns(() =>
        {
            tcsLastMoveNext.TrySetResult(false);

            return Task.CompletedTask;
        });
    }

    private class FailSerializer : ISerializer<int>
    {
        public byte[] Serialize(int data)
        {
            throw new Exception("Some serialize exception");
        }
    }

    [Fact]
    public async Task WriteAsync_WhenSerializeThrowException_ThrowWriterException()
    {
        await using var writer = new WriterBuilder<int>(_mockIDriver.Object, "/topic-1")
            { ProducerId = "producerId", Serializer = new FailSerializer() }.Build();

        _mockStream.Setup(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask);
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .Returns(_lastMoveNext);

        Assert.Equal("Error when serializing message data",
            (await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync(123))).Message);
    }

    /*
     * Performed invocations:

       Mock<IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "initRequest": { "path": "/topic-2", "producerId": "producerId" } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync() <- return false
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "initRequest": { "path": "/topic-2", "producerId": "producerId" } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync() <- return true
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "writeRequest": { "messages": [ { "seqNo": "1", "createdAt": "2024-12-03T10:46:43.954622Z", "data": "ZAAAAA==", "uncompressedSize": "4" } ], "codec": 1 } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
     */
    [Fact]
    public async Task Initialize_WhenStreamClosedByServer_ShouldRetryInitializeAndReturnWrittenMessageStatus()
    {
        var taskNextComplete = new TaskCompletionSource<bool>();
        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                taskNextComplete.SetResult(true);
                return Task.CompletedTask;
            });
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(false)
            .ReturnsAsync(true)
            .Returns(() => new ValueTask<bool>(taskNextComplete.Task))
            .Returns(_lastMoveNext);

        SetupReadOneWriteAckMessage();

        await using var writer = new WriterBuilder<int>(_mockIDriver.Object, "/topic-2")
            { ProducerId = "producerId" }.Build();

        Assert.Equal(PersistenceStatus.Written, (await writer.WriteAsync(100)).Status);

        // check attempt repeated!!!
        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(3));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.AtLeast(3));
        _mockStream.Verify(stream => stream.Current, Times.Exactly(2));
    }

    /*
     * Performed invocations:

       Mock<IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "initRequest": { "path": "/topic-3", "producerId": "producerId" } }) <- Driver.TransportException
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "initRequest": { "path": "/topic-3", "producerId": "producerId" } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync() <- return true
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync() <- return await write operation ValueTask
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "writeRequest": { "messages": [ { "seqNo": "1", "createdAt": "2024-12-03T10:59:47.408712Z", "data": "YWJhY2FiYQ==", "uncompressedSize": "7" } ], "codec": 1 } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync() <- sleep
     */
    [Fact]
    public async Task Initialize_WhenFailWriteMessage_ShouldRetryInitializeAndReturnWrittenMessageStatus()
    {
        var taskNextComplete = new TaskCompletionSource<bool>();
        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .ThrowsAsync(new Driver.TransportException(new RpcException(Grpc.Core.Status.DefaultCancelled)))
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                taskNextComplete.SetResult(true);
                return Task.CompletedTask;
            });
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .Returns(() => new ValueTask<bool>(taskNextComplete.Task))
            .Returns(_lastMoveNext);

        SetupReadOneWriteAckMessage();

        await using var writer = new WriterBuilder<string>(_mockIDriver.Object, "/topic-3")
            { ProducerId = "producerId" }.Build();

        Assert.Equal(PersistenceStatus.Written, (await writer.WriteAsync("abacaba")).Status);

        // check attempt repeated!!!
        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(3));
        _mockStream.Verify(stream => stream.MoveNextAsync(),
            Times.Between(2, 3, Range.Inclusive)); // run processing ack may not be able to start on time 
        _mockStream.Verify(stream => stream.Current, Times.Exactly(2));
    }

    /*
     * Performed invocations:

       Mock<IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "initRequest": { "path": "/topic-4", "producerId": "producerId" } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync() <- throw exception
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "initRequest": { "path": "/topic-4", "producerId": "producerId" } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "writeRequest": { "messages": [ { "seqNo": "1", "createdAt": "2024-12-03T11:07:42.201080Z", "data": "YWJhY2FiYQ==", "uncompressedSize": "7" } ], "codec": 1 } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
     */
    [Fact]
    public async Task Initialize_WhenFailMoveNextAsync_ShouldRetryInitializeAndReturnWrittenMessageStatus()
    {
        var taskNextComplete = new TaskCompletionSource<bool>();
        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                taskNextComplete.SetResult(true);
                return Task.CompletedTask;
            });
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ThrowsAsync(new Driver.TransportException(
                new RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.DeadlineExceeded, "Some message"))))
            .ReturnsAsync(true)
            .Returns(() => new ValueTask<bool>(taskNextComplete.Task))
            .Returns(_lastMoveNext);

        SetupReadOneWriteAckMessage();

        await using var writer = new WriterBuilder<string>(_mockIDriver.Object, "/topic-4")
            { ProducerId = "producerId" }.Build();

        Assert.Equal(PersistenceStatus.Written, (await writer.WriteAsync("abacaba")).Status);

        // check attempt repeated!!!
        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(3));
        _mockStream.Verify(stream => stream.MoveNextAsync(),
            Times.Between(3, 4, Range.Inclusive)); // run processing ack may not be able to start on time 
        _mockStream.Verify(stream => stream.Current, Times.Exactly(2));
    }

    /*
     * Performed invocations:

       Mock<IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "initRequest": { "path": "/topic-5", "producerId": "producerId" } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "initRequest": { "path": "/topic-5", "producerId": "producerId" } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "writeRequest": { "messages": [ { "seqNo": "1", "createdAt": "2024-12-03T11:42:03.516900Z", "data": "ewAAAAAAAAA=", "uncompressedSize": "8" } ], "codec": 1 } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
     */
    [Fact]
    public async Task Initialize_WhenInitResponseStatusIsRetryable_ShouldRetryInitializeAndReturnWrittenMessageStatus()
    {
        var taskNextComplete = new TaskCompletionSource<bool>();
        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                taskNextComplete.SetResult(true);
                return Task.CompletedTask;
            });
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(() => new ValueTask<bool>(taskNextComplete.Task))
            .Returns(_lastMoveNext);

        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                Status = StatusIds.Types.StatusCode.BadSession,
                Issues = { new IssueMessage { Message = "Some message" } }
            })
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                InitResponse = new StreamWriteMessage.Types.InitResponse
                    { LastSeqNo = 0, PartitionId = 1, SessionId = "SessionId" },
                Status = StatusIds.Types.StatusCode.Success
            })
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                WriteResponse = new StreamWriteMessage.Types.WriteResponse
                {
                    PartitionId = 1,
                    Acks =
                    {
                        new StreamWriteMessage.Types.WriteResponse.Types.WriteAck
                        {
                            SeqNo = 1,
                            Written = new StreamWriteMessage.Types.WriteResponse.Types.WriteAck.Types.Written
                                { Offset = 0 }
                        }
                    }
                },
                Status = StatusIds.Types.StatusCode.Success
            });

        await using var writer = new WriterBuilder<long>(_mockIDriver.Object, "/topic-5")
            { ProducerId = "producerId" }.Build();

        Assert.Equal(PersistenceStatus.Written, (await writer.WriteAsync(123L)).Status);

        // check attempt repeated!!!
        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(3));
        _mockStream.Verify(stream => stream.MoveNextAsync(),
            Times.Between(3, 4, Range.Inclusive)); // run processing ack may not be able to start on time 
        _mockStream.Verify(stream => stream.Current, Times.Exactly(3));
    }

    [Fact]
    public async Task
        Initialize_WhenInitResponseStatusIsNotRetryable_ThrowWriterExceptionOnWriteAsyncAndStopInitializing()
    {
        _mockStream.Setup(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask);
        _mockStream.Setup(stream => stream.MoveNextAsync())
            .Returns(new ValueTask<bool>(true));
        _mockStream.Setup(stream => stream.Current)
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                Status = StatusIds.Types.StatusCode.SchemeError,
                Issues = { new IssueMessage { Message = "Topic not found" } }
            });

        await using var writer = new WriterBuilder<long>(_mockIDriver.Object, "/topic-6")
            { ProducerId = "producerId" }.Build();

        Assert.Equal("Initialization failed: Status: SchemeError, Issues:\n[0] Fatal: Topic not found\n",
            (await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync(123L))).Message);
        Assert.Equal("Initialization failed: Status: SchemeError, Issues:\n[0] Fatal: Topic not found\n",
            (await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync(1L))).Message);

        // check not attempt repeated!!!
        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Once);
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Once);
    }

    [Fact]
    public async Task Initialize_WhenNotSupportedCodec_ThrowWriterExceptionOnWriteAsyncAndStopInitializing()
    {
        _mockStream.Setup(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask);
        _mockStream.Setup(stream => stream.MoveNextAsync())
            .Returns(new ValueTask<bool>(true));
        _mockStream.Setup(stream => stream.Current)
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                InitResponse = new StreamWriteMessage.Types.InitResponse
                {
                    LastSeqNo = 1, PartitionId = 1, SessionId = "SessionId",
                    SupportedCodecs = new SupportedCodecs { Codecs = { 2 /* Gzip */, 3 /* Lzop */ } }
                },
                Status = StatusIds.Types.StatusCode.Success
            });

        await using var writer = new WriterBuilder<long>(_mockIDriver.Object, "/topic-7")
            { ProducerId = "producerId", Codec = Codec.Raw }.Build();

        Assert.Equal("Topic[Path=\"/topic-7\"] is not supported codec: Raw",
            (await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync(123L))).Message);

        // check not attempt repeated!!!
        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Once);
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Once);
    }

    /*
     * Performed invocations:

       Mock<IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "initRequest": { "path": "/topic-8", "producerId": "producerId" } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "writeRequest": { "messages": [ { "seqNo": "1", "createdAt": "2024-12-03T12:44:23.276086Z", "data": "ZAAAAAAAAAA=", "uncompressedSize": "8" } ], "codec": 1 } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "initRequest": { "path": "/topic-8", "producerId": "producerId" } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "writeRequest": { "messages": [ { "seqNo": "1", "createdAt": "2024-12-03T12:44:23.276086Z", "data": "ZAAAAAAAAAA=", "uncompressedSize": "8" } ], "codec": 1 } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync() [Maybe]
     */
    [Fact]
    public async Task WriteAsync_WhenTransportExceptionOnWriteInWriterSession_ShouldReconnectAndReturnWriteResult()
    {
        var moveTcs = new TaskCompletionSource<bool>();

        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Throws(() =>
            {
                moveTcs.SetResult(false);
                return new Driver.TransportException(new RpcException(Grpc.Core.Status.DefaultCancelled));
            })
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask);
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(moveTcs.Task))
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(_lastMoveNext);
        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                InitResponse = new StreamWriteMessage.Types.InitResponse
                    { LastSeqNo = 0, PartitionId = 1, SessionId = "SessionId" },
                Status = StatusIds.Types.StatusCode.Success
            })
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                InitResponse = new StreamWriteMessage.Types.InitResponse
                    { LastSeqNo = 0, PartitionId = 1, SessionId = "SessionId" },
                Status = StatusIds.Types.StatusCode.Success
            })
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                WriteResponse = new StreamWriteMessage.Types.WriteResponse
                {
                    PartitionId = 1,
                    Acks =
                    {
                        new StreamWriteMessage.Types.WriteResponse.Types.WriteAck
                        {
                            SeqNo = 1,
                            Written = new StreamWriteMessage.Types.WriteResponse.Types.WriteAck.Types.Written
                                { Offset = 0 }
                        }
                    }
                },
                Status = StatusIds.Types.StatusCode.Success
            });
        await using var writer = new WriterBuilder<long>(_mockIDriver.Object, "/topic-8")
            { ProducerId = "producerId" }.Build();

        Assert.Equal(PersistenceStatus.Written, (await writer.WriteAsync(100L)).Status);
        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(4));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Between(4, 5, Range.Inclusive));
        _mockStream.Verify(stream => stream.Current, Times.Exactly(3));
    }

    /*
     * Performed invocations:

       Mock<IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "initRequest": { "path": "/topic-9", "producerId": "producerId" } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync() <- transport exception
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "initRequest": { "path": "/topic-9", "producerId": "producerId" } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync() <- return true
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync() <- return true after write message
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "writeRequest": { "messages": [ { "seqNo": "1", "createdAt": "2024-12-03T14:06:06.408114Z", "data": "ZAAAAAAAAAA=", "uncompressedSize": "8" } ], "codec": 1 } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync() [Maybe]
     */
    [Fact]
    public async Task WriteAsync_WhenTransportExceptionOnProcessingWriteAck_ShouldReconnectThenReturnWriteResult()
    {
        var moveTcs = new TaskCompletionSource<bool>();
        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                moveTcs.SetResult(true);
                return Task.CompletedTask;
            });
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ThrowsAsync(new Driver.TransportException(new RpcException(Grpc.Core.Status.DefaultCancelled)))
            .ReturnsAsync(true)
            .Returns(() => new ValueTask<bool>(moveTcs.Task)) // retry init writer session
            .Returns(_lastMoveNext);
        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                InitResponse = new StreamWriteMessage.Types.InitResponse
                    { LastSeqNo = 0, PartitionId = 1, SessionId = "SessionId" },
                Status = StatusIds.Types.StatusCode.Success
            })
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                InitResponse = new StreamWriteMessage.Types.InitResponse
                    { LastSeqNo = 0, PartitionId = 1, SessionId = "SessionId" },
                Status = StatusIds.Types.StatusCode.Success
            })
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                WriteResponse = new StreamWriteMessage.Types.WriteResponse
                {
                    PartitionId = 1,
                    Acks =
                    {
                        new StreamWriteMessage.Types.WriteResponse.Types.WriteAck
                        {
                            SeqNo = 1,
                            Written = new StreamWriteMessage.Types.WriteResponse.Types.WriteAck.Types.Written
                                { Offset = 0 }
                        }
                    }
                },
                Status = StatusIds.Types.StatusCode.Success
            });
        await using var writer = new WriterBuilder<long>(_mockIDriver.Object, "/topic-9")
            { ProducerId = "producerId" }.Build();

        Assert.Equal(PersistenceStatus.Written, (await writer.WriteAsync(100L)).Status);

        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(3));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Between(4, 5, Range.Inclusive));
        _mockStream.Verify(stream => stream.Current, Times.Exactly(3));
    }

    /*
     * Performed invocations:

       Mock<IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "initRequest": { "path": "/topic-10", "producerId": "producerId" } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "initRequest": { "path": "/topic-10", "producerId": "producerId" } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "writeRequest": { "messages": [ { "seqNo": "1", "createdAt": "2024-12-03T14:12:59.548210Z", "data": "ZAAAAAAAAAA=", "uncompressedSize": "8" } ], "codec": 1 } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync() [Maybe]
     */
    [Fact]
    public async Task WriteAsync_WhenStreamIsClosingOnProcessingWriteAck_ShouldReconnectThenReturnWriteResult()
    {
        var moveTcs = new TaskCompletionSource<bool>();
        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                moveTcs.SetResult(true);
                return Task.CompletedTask;
            });
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(false)
            .ReturnsAsync(true)
            .Returns(() => new ValueTask<bool>(moveTcs.Task)) // retry init writer session
            .Returns(_lastMoveNext);
        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                InitResponse = new StreamWriteMessage.Types.InitResponse
                    { LastSeqNo = 0, PartitionId = 1, SessionId = "SessionId" },
                Status = StatusIds.Types.StatusCode.Success
            })
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                InitResponse = new StreamWriteMessage.Types.InitResponse
                    { LastSeqNo = 0, PartitionId = 1, SessionId = "SessionId" },
                Status = StatusIds.Types.StatusCode.Success
            })
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                WriteResponse = new StreamWriteMessage.Types.WriteResponse
                {
                    PartitionId = 1,
                    Acks =
                    {
                        new StreamWriteMessage.Types.WriteResponse.Types.WriteAck
                        {
                            SeqNo = 1,
                            Written = new StreamWriteMessage.Types.WriteResponse.Types.WriteAck.Types.Written
                                { Offset = 0 }
                        }
                    }
                },
                Status = StatusIds.Types.StatusCode.Success
            });

        await using var writer = new WriterBuilder<long>(_mockIDriver.Object, "/topic-10")
            { ProducerId = "producerId" }.Build();


        Assert.Equal(PersistenceStatus.Written, (await writer.WriteAsync(100L)).Status);

        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(3));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Between(4, 5, Range.Inclusive));
        _mockStream.Verify(stream => stream.Current, Times.Exactly(3));
    }

    [Fact]
    public async Task WriteAsync_WhenCancellationTokenIsClosed_ThrowCancellationException()
    {
        var cancellationTokenSource = new CancellationTokenSource();
        var nextCompleted = new TaskCompletionSource<bool>();
        _mockStream.Setup(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask);
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .Returns(_lastMoveNext);
        SetupReadOneWriteAckMessage();

        await using var writer = new WriterBuilder<long>(_mockIDriver.Object, "/topic-11")
            { ProducerId = "producerId" }.Build();

        var task = writer.WriteAsync(123L, cancellationTokenSource.Token);
        cancellationTokenSource.Cancel();
        nextCompleted.SetResult(true);

        Assert.Equal("The write operation was canceled before it could be completed",
            (await Assert.ThrowsAsync<WriterException>(() => task)).Message);
    }

    [Fact]
    public async Task WriteAsync_WhenTaskIsAcceptedBeforeCancel_ReturnWrittenStatus()
    {
        var cancellationTokenSource = new CancellationTokenSource();
        var nextCompleted = new TaskCompletionSource<bool>();
        _mockStream.Setup(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask);
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(nextCompleted.Task))
            .Returns(_lastMoveNext);
        SetupReadOneWriteAckMessage();

        await using var writer = new WriterBuilder<long>(_mockIDriver.Object, "/topic-12")
            { ProducerId = "producerId" }.Build();

        var task = writer.WriteAsync(123L, cancellationTokenSource.Token);
        nextCompleted.SetResult(true);
        Assert.Equal(PersistenceStatus.Written, (await task).Status);
        cancellationTokenSource.Cancel();
    }


    /*
     * Performed invocations:

       Mock<IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "initRequest": { "path": "/topic-13", "producerId": "producerId" } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "writeRequest": { "messages": [ { "seqNo": "1", "createdAt": "2024-12-03T15:43:34.479478Z", "data": "ZAAAAAAAAAA=", "uncompressedSize": "8" } ], "codec": 1 } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "writeRequest": { "messages": [ { "seqNo": "2", "createdAt": "2024-12-03T15:43:34.481385Z", "data": "ZAAAAAAAAAA=", "uncompressedSize": "8" } ], "codec": 1 } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "writeRequest": { "messages": [ { "seqNo": "3", "createdAt": "2024-12-03T15:43:34.481425Z", "data": "ZAAAAAAAAAA=", "uncompressedSize": "8" } ], "codec": 1 } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "initRequest": { "path": "/topic-13", "producerId": "producerId" } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "writeRequest": { "messages": [ { "seqNo": "3", "createdAt": "2024-12-03T15:43:34.481425Z", "data": "ZAAAAAAAAAA=", "uncompressedSize": "8" } ], "codec": 1 } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
     */
    [Fact]
    public async Task WriteAsync_WhenInFlightBufferSendInInitialize_ReturnCompletedTasks()
    {
        var writeTcs1 = new TaskCompletionSource();
        var writeTcs2 = new TaskCompletionSource();
        var writeTcs3 = new TaskCompletionSource();
        var moveTcs = new TaskCompletionSource<bool>();

        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                writeTcs1.SetResult();
                return Task.CompletedTask;
            })
            .Returns(() =>
            {
                writeTcs2.SetResult();
                return Task.CompletedTask;
            })
            .Returns(() =>
            {
                writeTcs3.SetResult();
                return Task.CompletedTask;
            })
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask);

        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(moveTcs.Task))
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(_lastMoveNext);
        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                InitResponse = new StreamWriteMessage.Types.InitResponse
                    { LastSeqNo = 0, PartitionId = 1, SessionId = "SessionId" },
                Status = StatusIds.Types.StatusCode.Success
            })
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                InitResponse = new StreamWriteMessage.Types.InitResponse
                    { LastSeqNo = 2, PartitionId = 1, SessionId = "SessionId" },
                Status = StatusIds.Types.StatusCode.Success
            })
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                WriteResponse = new StreamWriteMessage.Types.WriteResponse
                {
                    PartitionId = 1,
                    Acks =
                    {
                        new StreamWriteMessage.Types.WriteResponse.Types.WriteAck
                        {
                            SeqNo = 3,
                            Written = new StreamWriteMessage.Types.WriteResponse.Types.WriteAck.Types.Written
                                { Offset = 0 }
                        }
                    }
                },
                Status = StatusIds.Types.StatusCode.Success
            });
        await using var writer = new WriterBuilder<long>(_mockIDriver.Object, "/topic-13")
            { ProducerId = "producerId" }.Build();

        var ctx = new CancellationTokenSource();
        var runTaskWithCancel = writer.WriteAsync(100L, ctx.Token);
        await writeTcs1.Task;
        ctx.Cancel(); // reconnect write invoke cancel on cancellation token

        // ReSharper disable once MethodSupportsCancellation
        var runTask1 = writer.WriteAsync(100L);
        await writeTcs2.Task;

        // ReSharper disable once MethodSupportsCancellation
        var runTask2 = writer.WriteAsync(100);
        await writeTcs3.Task;

        moveTcs.SetResult(false); // Fail write ack stream => start reconnect

        Assert.Equal("The write operation was canceled before it could be completed",
            (await Assert.ThrowsAsync<WriterException>(() => runTaskWithCancel)).Message);
        Assert.Equal(PersistenceStatus.AlreadyWritten, (await runTask1).Status);
        Assert.Equal(PersistenceStatus.Written, (await runTask2).Status);

        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(6));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Between(4, 5, Range.Inclusive));
        _mockStream.Verify(stream => stream.Current, Times.Exactly(3));
    }

    [Fact]
    public async Task WriteAsync_WhenWriterIsDisposed_ThrowWriterException()
    {
        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask);
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true);
        SetupReadOneWriteAckMessage();

        var writer = new WriterBuilder<string>(_mockIDriver.Object, "/topic-14")
            { ProducerId = "producerId" }.Build();
        await writer.DisposeAsync();

        Assert.Equal("Writer[TopicPath: /topic-14, ProducerId: producerId, Codec: Raw] is disposed",
            (await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync("abacaba"))).Message);
    }

    /*
     * Performed invocations:

       Mock<IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "initRequest": { "path": "/topic-14", "producerId": "producerId" } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.AuthToken
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.AuthToken
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "writeRequest": { "messages": [ { "seqNo": "1", "createdAt": "2025-03-03T11:07:14.079309Z", "data": "ZAAAAAAAAAA=", "uncompressedSize": "8" } ], "codec": 1 } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.AuthToken
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "updateTokenRequest": { "token": "Token2" } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "writeRequest": { "messages": [ { "seqNo": "2", "createdAt": "2025-03-03T11:07:14.084021Z", "data": "ZAAAAAAAAAA=", "uncompressedSize": "8" } ], "codec": 1 } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.AuthToken
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "writeRequest": { "messages": [ { "seqNo": "3", "createdAt": "2025-03-03T11:07:14.084187Z", "data": "ZAAAAAAAAAA=", "uncompressedSize": "8" } ], "codec": 1 } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
     */
    [Fact]
    public async Task WriteAsync_WhenTokenIsUpdatedOneTime_SuccessUpdateToken()
    {
        var writeTcs1 = new TaskCompletionSource<bool>();
        var writeTcs2 = new TaskCompletionSource<bool>();
        var writeTcs3 = new TaskCompletionSource<bool>();

        _mockStream.SetupSequence(stream => stream.AuthToken)
            .ReturnsAsync("Token1")
            .ReturnsAsync("Token1")
            .ReturnsAsync("Token2")
            .ReturnsAsync("Token2");

        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                writeTcs1.SetResult(true);
                return Task.CompletedTask;
            })
            .Returns(Task.CompletedTask) // send token update
            .Returns(() =>
            {
                writeTcs2.SetResult(true);
                return Task.CompletedTask;
            })
            .Returns(() =>
            {
                writeTcs3.SetResult(true);
                return Task.CompletedTask;
            });

        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(writeTcs1.Task))
            .Returns(new ValueTask<bool>(writeTcs2.Task))
            .Returns(new ValueTask<bool>(writeTcs3.Task))
            .Returns(_lastMoveNext);

        SetupReadOneWriteAckMessage()
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                WriteResponse = new StreamWriteMessage.Types.WriteResponse
                {
                    PartitionId = 1,
                    Acks =
                    {
                        new StreamWriteMessage.Types.WriteResponse.Types.WriteAck
                        {
                            SeqNo = 2,
                            Written = new StreamWriteMessage.Types.WriteResponse.Types.WriteAck.Types.Written
                                { Offset = 2 }
                        }
                    }
                },
                Status = StatusIds.Types.StatusCode.Success
            })
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                WriteResponse = new StreamWriteMessage.Types.WriteResponse
                {
                    PartitionId = 1,
                    Acks =
                    {
                        new StreamWriteMessage.Types.WriteResponse.Types.WriteAck
                        {
                            SeqNo = 3,
                            Written = new StreamWriteMessage.Types.WriteResponse.Types.WriteAck.Types.Written
                                { Offset = 3 }
                        }
                    }
                },
                Status = StatusIds.Types.StatusCode.Success
            });

        await using var writer = new WriterBuilder<long>(_mockIDriver.Object, "/topic-15")
            { ProducerId = "producerId" }.Build();

        var writeTask1 = await writer.WriteAsync(100L);
        Assert.Equal(PersistenceStatus.Written, writeTask1.Status);

        var writeTask2 = await writer.WriteAsync(100);
        Assert.Equal(PersistenceStatus.Written, writeTask2.Status);

        var writeTask3 = await writer.WriteAsync(100);
        Assert.Equal(PersistenceStatus.Written, writeTask3.Status);

        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Between(4, 5, Range.Inclusive));
        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(5));
        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(msg =>
            msg.UpdateTokenRequest != null &&
            msg.UpdateTokenRequest.Token == "Token2")));
    }

    [Fact]
    public async Task DisposeAsync_WhenInFlightMessages_WaitingInFlightMessages()
    {
        var tcsDetectedWrite = new TaskCompletionSource();
        var writeTcs1 = new TaskCompletionSource<bool>();

        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                tcsDetectedWrite.TrySetResult();
                return Task.CompletedTask;
            })
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask);
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(writeTcs1.Task))
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(_lastMoveNext);

        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                InitResponse = new StreamWriteMessage.Types.InitResponse
                    { LastSeqNo = 0, PartitionId = 1, SessionId = "SessionId" },
                Status = StatusIds.Types.StatusCode.Success
            })
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                InitResponse = new StreamWriteMessage.Types.InitResponse
                    { LastSeqNo = 0, PartitionId = 1, SessionId = "SessionId" },
                Status = StatusIds.Types.StatusCode.Success
            })
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                WriteResponse = new StreamWriteMessage.Types.WriteResponse
                {
                    PartitionId = 1,
                    Acks =
                    {
                        new StreamWriteMessage.Types.WriteResponse.Types.WriteAck
                        {
                            SeqNo = 1,
                            Written = new StreamWriteMessage.Types.WriteResponse.Types.WriteAck.Types.Written
                                { Offset = 0 }
                        }
                    }
                },
                Status = StatusIds.Types.StatusCode.Success
            });

        var writer = new WriterBuilder<long>(_mockIDriver.Object, "/topic-16")
            { ProducerId = "producerId" }.Build();

        var writeTask1 = writer.WriteAsync(100L);

        await tcsDetectedWrite.Task;
        var disposedTask = writer.DisposeAsync();

        Assert.False(writeTask1.IsCompleted);
        Assert.False(disposedTask.IsCompleted);
        writeTcs1.TrySetException(new Driver.TransportException(
            new RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.DeadlineExceeded, "Some message"))));
        Assert.Equal("Writer[TopicPath: /topic-16, ProducerId: producerId, Codec: Raw] is disposed",
            (await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync(12))).Message);

        Assert.Equal(PersistenceStatus.Written, (await writeTask1).Status);

        Assert.Equal("Writer[TopicPath: /topic-16, ProducerId: producerId, Codec: Raw] is disposed",
            (await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync(12))).Message);

        await disposedTask;

        // idempotent
        await writer.DisposeAsync();
    }

    private ISetupSequentialResult<StreamWriteMessage.Types.FromServer> SetupReadOneWriteAckMessage()
    {
        return _mockStream.SetupSequence(stream => stream.Current)
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                InitResponse = new StreamWriteMessage.Types.InitResponse
                    { LastSeqNo = 0, PartitionId = 1, SessionId = "SessionId" },
                Status = StatusIds.Types.StatusCode.Success
            })
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                WriteResponse = new StreamWriteMessage.Types.WriteResponse
                {
                    PartitionId = 1,
                    Acks =
                    {
                        new StreamWriteMessage.Types.WriteResponse.Types.WriteAck
                        {
                            SeqNo = 1,
                            Written = new StreamWriteMessage.Types.WriteResponse.Types.WriteAck.Types.Written
                                { Offset = 0 }
                        }
                    }
                },
                Status = StatusIds.Types.StatusCode.Success
            });
    }
}
