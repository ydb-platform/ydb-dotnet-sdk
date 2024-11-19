using Grpc.Core;
using Moq;
using Xunit;
using Ydb.Sdk.Services.Topic;
using Ydb.Sdk.Services.Topic.Writer;
using Ydb.Topic;

namespace Ydb.Sdk.Tests.Topic;

using WriterStream = BidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>;

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
    }

    // [Fact]
    public async Task NotStarted_Failed_Test()
    {
        var moveNextTry = new TaskCompletionSource<bool>();

        _mockStream
            .Setup(stream => stream.Write(It.IsAny<StreamWriteMessage.Types.FromClient>()))
            .Returns(Task.CompletedTask);

        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(false)
            .Returns(new ValueTask<bool>(moveNextTry.Task)); // For retry

        using var writer = new WriterBuilder<int>(_mockIDriver.Object, new WriterConfig("/topic")
            { ProducerId = "producerId" }).Build();

        await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync(100));
    }
}
