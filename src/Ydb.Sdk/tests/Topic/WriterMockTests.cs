using Grpc.Core;
using Moq;
using Xunit;
using Xunit.Abstractions;
using Ydb.Issue;
using Ydb.Sdk.Services.Topic;
using Ydb.Sdk.Services.Topic.Writer;
using Ydb.Topic;
using Codec = Ydb.Sdk.Services.Topic.Codec;

namespace Ydb.Sdk.Tests.Topic;

using WriterStream = IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>;
using FromClient = StreamWriteMessage.Types.FromClient;

public class WriterMockTests
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly Mock<IDriver> _mockIDriver = new();
    private readonly Mock<WriterStream> _mockStream = new();

    public WriterMockTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;
        _mockIDriver.Setup(driver => driver.BidirectionalStreamCall(
            It.IsAny<Method<FromClient, StreamWriteMessage.Types.FromServer>>(),
            It.IsAny<GrpcRequestSettings>())
        ).Returns(_mockStream.Object);

        _mockIDriver.Setup(driver => driver.LoggerFactory).Returns(Utils.GetLoggerFactory);
    }

    [Fact]
    public async Task Initialize_WhenStreamIsClosedByServer_ThrowWriterExceptionOnWriteAsyncAndTryNextInitialize()
    {
        var moveNextTry = new TaskCompletionSource<bool>();
        var taskNextComplete = new TaskCompletionSource();

        _mockStream.Setup(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask);
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(false)
            .Returns(() =>
            {
                taskNextComplete.SetResult();
                return new ValueTask<bool>(moveNextTry.Task);
            });

        using var writer = new WriterBuilder<int>(_mockIDriver.Object, new WriterConfig("/topic")
            { ProducerId = "producerId" }).Build();

        Assert.Equal("Stream unexpectedly closed by YDB server. " +
                     "Current InitRequest: { \"path\": \"/topic\", \"producerId\": \"producerId\" }",
            (await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync(100))).Message);

        await taskNextComplete.Task;
        // check attempt repeated!!!
        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(2));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Exactly(2));
    }

    [Fact]
    public async Task Initialize_WhenFailWriteMessage_ThrowWriterExceptionOnWriteAsyncAndTryNextInitialize()
    {
        var taskSource = new TaskCompletionSource();
        var taskNextComplete = new TaskCompletionSource();
        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .ThrowsAsync(new Driver.TransportException(new RpcException(Grpc.Core.Status.DefaultCancelled)))
            .Returns(() =>
            {
                taskNextComplete.SetResult();
                return taskSource.Task;
            });

        using var writer = new WriterBuilder<string>(_mockIDriver.Object, new WriterConfig("/topic")
            { ProducerId = "producerId" }).Build();

        var writerException = await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync("abacaba"));
        Assert.Equal("Transport error on creating WriterSession", writerException.Message);
        Assert.Equal(StatusCode.Cancelled, writerException.Status.StatusCode);

        await taskNextComplete.Task;
        // check attempt repeated!!!
        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(2));
    }

    [Fact]
    public async Task Initialize_WhenFailMoveNextAsync_ThrowWriterExceptionOnWriteAsyncAndTryNextInitialize()
    {
        var taskSource = new TaskCompletionSource<bool>();
        var taskNextComplete = new TaskCompletionSource();
        _mockStream.Setup(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask);
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ThrowsAsync(new Driver.TransportException(
                new RpcException(new Grpc.Core.Status(Grpc.Core.StatusCode.DeadlineExceeded, "Some message"))))
            .Returns(() =>
            {
                taskNextComplete.SetResult();
                return new ValueTask<bool>(taskSource.Task);
            });

        using var writer = new WriterBuilder<string>(_mockIDriver.Object, new WriterConfig("/topic")
            { ProducerId = "producerId" }).Build();

        var writerException = await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync("abacaba"));
        Assert.Equal("Transport error on creating WriterSession", writerException.Message);
        Assert.Equal(StatusCode.ClientTransportTimeout, writerException.Status.StatusCode);

        await taskNextComplete.Task;
        // check attempt repeated!!!
        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(2));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Exactly(2));
    }

    [Fact]
    public async Task Initialize_WhenInitResponseNotSuccess_ThrowWriterExceptionOnWriteAsyncAndTryNextInitialize()
    {
        var taskSource = new TaskCompletionSource<bool>();
        var taskNextComplete = new TaskCompletionSource();
        _mockStream.Setup(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask);
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .Returns(new ValueTask<bool>(true))
            .Returns(() =>
            {
                taskNextComplete.SetResult();
                return new ValueTask<bool>(taskSource.Task);
            });
        _mockStream.Setup(stream => stream.Current)
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                Status = StatusIds.Types.StatusCode.BadSession,
                Issues = { new IssueMessage { Message = "Some message" } }
            });

        using var writer = new WriterBuilder<long>(_mockIDriver.Object, new WriterConfig("/topic")
            { ProducerId = "producerId" }).Build();

        Assert.Equal("Initialization failed: Status: BadSession, Issues:\n[0] Fatal: Some message\n",
            (await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync(123L))).Message);

        await taskNextComplete.Task;
        // check attempt repeated!!!
        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(2));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Exactly(2));
    }

    [Fact]
    public async Task Initialize_WhenInitResponseIsSchemaError_ThrowWriterExceptionOnWriteAsyncAndStopInitializing()
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

        using var writer = new WriterBuilder<long>(_mockIDriver.Object, new WriterConfig("/topic")
            { ProducerId = "producerId" }).Build();

        Assert.Equal("Initialization failed: Status: SchemeError, Issues:\n[0] Fatal: Topic not found\n",
            (await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync(123L))).Message);

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

        using var writer = new WriterBuilder<long>(_mockIDriver.Object, new WriterConfig("/topic")
            { ProducerId = "producerId", Codec = Codec.Raw }).Build();

        Assert.Equal("Topic[Path=\"/topic\"] is not supported codec: Raw",
            (await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync(123L))).Message);

        // check not attempt repeated!!!
        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Once);
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Once);
    }

    [Fact]
    public async Task WriteAsyncStress_WhenBufferIsOverflow_ThrowWriterExceptionOnBufferOverflow()
    {
        const int countBatchSendingSize = 1000;
        const int batchTasksSize = 100;
        const int bufferSize = 100;
        const int messageSize = sizeof(int);

        Assert.True(batchTasksSize > bufferSize / 4);
        Assert.True(bufferSize % 4 == 0);

        var taskSource = new TaskCompletionSource<bool>();
        _mockStream.Setup(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask);
        var mockNextAsync = _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .Returns(new ValueTask<bool>(true))
            .Returns(new ValueTask<bool>(taskSource.Task));
        var sequentialResult = _mockStream.SetupSequence(stream => stream.Current)
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                InitResponse = new StreamWriteMessage.Types.InitResponse
                    { LastSeqNo = 0, PartitionId = 1, SessionId = "SessionId" },
                Status = StatusIds.Types.StatusCode.Success
            });
        using var writer = new WriterBuilder<int>(_mockIDriver.Object, new WriterConfig("/topic")
            { ProducerId = "producerId", BufferMaxSize = bufferSize /* bytes */ }).Build();

        for (var attempt = 0; attempt < countBatchSendingSize; attempt++)
        {
            _testOutputHelper.WriteLine($"Processing attempt {attempt}");

            var tasks = new List<Task<WriteResult>>();
            var serverAck = new StreamWriteMessage.Types.FromServer
            {
                WriteResponse = new StreamWriteMessage.Types.WriteResponse { PartitionId = 1 },
                Status = StatusIds.Types.StatusCode.Success
            };
            for (var i = 0; i < batchTasksSize; i++)
            {
                tasks.Add(writer.WriteAsync(100));
                serverAck.WriteResponse.Acks.Add(new StreamWriteMessage.Types.WriteResponse.Types.WriteAck
                {
                    SeqNo = bufferSize / messageSize * attempt + i + 1,
                    Written = new StreamWriteMessage.Types.WriteResponse.Types.WriteAck.Types.Written
                        { Offset = i * messageSize + bufferSize * attempt }
                });
            }

            sequentialResult.Returns(() =>
            {
                // ReSharper disable once AccessToModifiedClosure
                Volatile.Write(ref taskSource, new TaskCompletionSource<bool>());
                mockNextAsync.Returns(new ValueTask<bool>(Volatile.Read(ref taskSource).Task));
                return serverAck;
            });
            taskSource.SetResult(true);

            var countSuccess = 0;
            var countErrors = 0;
            foreach (var task in tasks)
            {
                try
                {
                    var res = await task;
                    countSuccess++;
                    Assert.Equal(PersistenceStatus.Written, res.Status);
                }
                catch (WriterException e)
                {
                    countErrors++;
                    Assert.Equal("Buffer overflow", e.Message);
                }
            }

            Assert.Equal(bufferSize / messageSize, countSuccess);
            Assert.Equal(batchTasksSize - bufferSize / messageSize, countErrors);
        }
    }

    /*
     * Performed invocations:

       Mock<IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>:1> (stream):

          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "initRequest": { "path": "/topic", "producerId": "producerId" } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "writeRequest": { "messages": [ { "seqNo": "1", "createdAt": "2024-11-22T10:08:58.732882Z", "data": "AAAAAAAAAGQ=", "uncompressedSize": "8" } ], "codec": 1 } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "initRequest": { "path": "/topic", "producerId": "producerId" } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Write({ "writeRequest": { "messages": [ { "seqNo": "1", "createdAt": "2024-11-22T10:08:58.732882Z", "data": "AAAAAAAAAGQ=", "uncompressedSize": "8" } ], "codec": 1 } })
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.Current
          IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>.MoveNextAsync()
     */
    [Fact]
    public async Task WriteAsync_WhenTransportExceptionOnWriteInWriterSessionThenReconnectSession_ReturnWriteResult()
    {
        var moveFirstNextSource = new TaskCompletionSource<bool>();
        var moveSecondNextSource = new TaskCompletionSource<bool>();
        var moveThirdNextSource = new TaskCompletionSource<bool>();
        var nextCompleted = new TaskCompletionSource();
        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .ThrowsAsync(new Driver.TransportException(new RpcException(Grpc.Core.Status.DefaultCancelled)))
            .Returns(() =>
            {
                moveFirstNextSource.SetResult(false);
                return Task.CompletedTask;
            })
            .Returns(() =>
            {
                nextCompleted.SetResult();
                return Task.CompletedTask;
            });
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .Returns(new ValueTask<bool>(true))
            .Returns(new ValueTask<bool>(moveFirstNextSource.Task))
            .Returns(new ValueTask<bool>(moveSecondNextSource.Task))
            .Returns(new ValueTask<bool>(moveThirdNextSource.Task))
            .Returns(new ValueTask<bool>(new TaskCompletionSource<bool>().Task));
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
        using var writer = new WriterBuilder<long>(_mockIDriver.Object, new WriterConfig("/topic")
            { ProducerId = "producerId" }).Build();

        var runTask = writer.WriteAsync(100L);

        var writerExceptionAfterResetSession = await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync(100));
        Assert.Equal("Transport error in the WriterSession on write messages",
            writerExceptionAfterResetSession.Message);
        Assert.Equal(StatusCode.Cancelled, writerExceptionAfterResetSession.Status.StatusCode);

        moveSecondNextSource.SetResult(true);
        await nextCompleted.Task;
        moveThirdNextSource.SetResult(true);

        Assert.Equal(PersistenceStatus.Written, (await runTask).Status);
        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(4));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Exactly(5));
        _mockStream.Verify(stream => stream.Current, Times.Exactly(3));
    }
}
