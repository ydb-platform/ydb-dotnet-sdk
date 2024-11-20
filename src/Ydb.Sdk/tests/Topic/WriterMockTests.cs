using Grpc.Core;
using Moq;
using Xunit;
using Ydb.Issue;
using Ydb.Sdk.Services.Topic;
using Ydb.Sdk.Services.Topic.Writer;
using Ydb.Topic;
using Codec = Ydb.Sdk.Services.Topic.Codec;

namespace Ydb.Sdk.Tests.Topic;

using WriterStream = IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>;

public class WriterMockTests
{
    private readonly Mock<IDriver> _mockIDriver = new();
    private readonly Mock<WriterStream> _mockStream = new();

    public WriterMockTests()
    {
        _mockIDriver.Setup(driver => driver.BidirectionalStreamCall(
            It.IsAny<Method<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>>(),
            It.IsAny<GrpcRequestSettings>())
        ).Returns(_mockStream.Object);

        _mockIDriver.Setup(driver => driver.LoggerFactory).Returns(Utils.GetLoggerFactory);
    }

    [Fact]
    public async Task Initialize_WhenStreamIsClosedByServer_ThrowWriterExceptionOnWriteAsyncAndTryNextInitialize()
    {
        var moveNextTry = new TaskCompletionSource<bool>();
        var taskNextComplete = new TaskCompletionSource();

        _mockStream.Setup(stream => stream.Write(It.IsAny<StreamWriteMessage.Types.FromClient>()))
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
        _mockStream.Verify(stream => stream.Write(It.IsAny<StreamWriteMessage.Types.FromClient>()), Times.Exactly(2));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Exactly(2));
    }

    [Fact]
    public async Task Initialize_WhenFailWriteMessage_ThrowWriterExceptionOnWriteAsyncAndTryNextInitialize()
    {
        var taskSource = new TaskCompletionSource();
        var taskNextComplete = new TaskCompletionSource();
        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<StreamWriteMessage.Types.FromClient>()))
            .ThrowsAsync(new Driver.TransportException(new RpcException(Grpc.Core.Status.DefaultCancelled)))
            .Returns(() =>
            {
                taskNextComplete.SetResult();
                return taskSource.Task;
            });

        using var writer = new WriterBuilder<string>(_mockIDriver.Object, new WriterConfig("/topic")
            { ProducerId = "producerId" }).Build();

        var writerException = await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync("abacaba"));
        Assert.Equal("Transport error on creating write session", writerException.Message);
        Assert.Equal(StatusCode.Cancelled, writerException.Status.StatusCode);

        await taskNextComplete.Task;
        // check attempt repeated!!!
        _mockStream.Verify(stream => stream.Write(It.IsAny<StreamWriteMessage.Types.FromClient>()), Times.Exactly(2));
    }

    [Fact]
    public async Task Initialize_WhenFailMoveNextAsync_ThrowWriterExceptionOnWriteAsyncAndTryNextInitialize()
    {
        var taskSource = new TaskCompletionSource<bool>();
        var taskNextComplete = new TaskCompletionSource();
        _mockStream.Setup(stream => stream.Write(It.IsAny<StreamWriteMessage.Types.FromClient>()))
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
        Assert.Equal("Transport error on creating write session", writerException.Message);
        Assert.Equal(StatusCode.ClientTransportTimeout, writerException.Status.StatusCode);

        await taskNextComplete.Task;
        // check attempt repeated!!!
        _mockStream.Verify(stream => stream.Write(It.IsAny<StreamWriteMessage.Types.FromClient>()), Times.Exactly(2));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Exactly(2));
    }

    [Fact]
    public async Task Initialize_WhenInitResponseNotSuccess_ThrowWriterExceptionOnWriteAsyncAndTryNextInitialize()
    {
        var taskSource = new TaskCompletionSource<bool>();
        var taskNextComplete = new TaskCompletionSource();
        _mockStream.Setup(stream => stream.Write(It.IsAny<StreamWriteMessage.Types.FromClient>()))
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
        _mockStream.Verify(stream => stream.Write(It.IsAny<StreamWriteMessage.Types.FromClient>()), Times.Exactly(2));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Exactly(2));
    }

    [Fact]
    public async Task Initialize_WhenInitResponseIsSchemaError_ThrowWriterExceptionOnWriteAsyncAndStopInitializing()
    {
        _mockStream.Setup(stream => stream.Write(It.IsAny<StreamWriteMessage.Types.FromClient>()))
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
        _mockStream.Verify(stream => stream.Write(It.IsAny<StreamWriteMessage.Types.FromClient>()), Times.Once);
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Once);
    }

    [Fact]
    public async Task Initialize_WhenNotSupportedCodec_ThrowWriterExceptionOnWriteAsyncAndStopInitializing()
    {
        _mockStream.Setup(stream => stream.Write(It.IsAny<StreamWriteMessage.Types.FromClient>()))
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
                Status = StatusIds.Types.StatusCode.Success,
            });

        using var writer = new WriterBuilder<long>(_mockIDriver.Object, new WriterConfig("/topic")
            { ProducerId = "producerId", Codec = Codec.Raw }).Build();

        Assert.Equal("Topic[Path=\"/topic\"] is not supported codec: Raw",
            (await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync(123L))).Message);

        // check not attempt repeated!!!
        _mockStream.Verify(stream => stream.Write(It.IsAny<StreamWriteMessage.Types.FromClient>()), Times.Once);
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Once);
    }
    
    

    /*
     *         _mockStream.Setup(stream => stream.Current)
           .Returns(new StreamWriteMessage.Types.FromServer
           {
               InitResponse = new StreamWriteMessage.Types.InitResponse
                   { LastSeqNo = 1, PartitionId = 1, SessionId = "SessionId" },
               Status = StatusIds.Types.StatusCode.Success,
           });
       moveNextTry.SetResult(true);
       await Task.Yield();

       var writeTask = writer.WriteAsync(100);
       moveNextTryWriteAck.SetResult(true);

       _mockStream.Setup(stream => stream.Current).Returns(
           new StreamWriteMessage.Types.FromServer
           {
               WriteResponse = new StreamWriteMessage.Types.WriteResponse
               {
                   Acks =
                   {
                       new StreamWriteMessage.Types.WriteResponse.Types.WriteAck
                       {
                           SeqNo = 1, Written =
                               new StreamWriteMessage.Types.WriteResponse.Types.WriteAck.Types.Written
                                   { Offset = 2 }
                       }
                   }
               },
               Status = StatusIds.Types.StatusCode.Success
           });
       _mockStream.Setup(stream => stream.MoveNextAsync()).ReturnsAsync(true);

       var writeResult = await writeTask;
       Assert.Equal(PersistenceStatus.Written, writeResult.Status);
       Assert.True(writeResult.TryGetOffset(out var offset));
       Assert.Equal(2, offset);
       _mockStream.Setup(stream => stream.MoveNextAsync()).ReturnsAsync(false);
     */
}
