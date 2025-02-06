using Google.Protobuf;
using Grpc.Core;
using Moq;
using Xunit;
using Ydb.Sdk.Services.Topic.Reader;
using Ydb.Topic;

namespace Ydb.Sdk.Tests.Topic;

using ReaderStream = IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>;
using FromClient = StreamReadMessage.Types.FromClient;
using FromServer = StreamReadMessage.Types.FromServer;

public class ReaderUnitTests
{
    private readonly Mock<IDriver> _mockIDriver = new();
    private readonly Mock<ReaderStream> _mockStream = new();

    public ReaderUnitTests()
    {
        _mockIDriver.Setup(driver => driver.BidirectionalStreamCall(
            It.IsAny<Method<FromClient, FromServer>>(),
            It.IsAny<GrpcRequestSettings>())
        ).Returns(_mockStream.Object);

        _mockIDriver.Setup(driver => driver.LoggerFactory).Returns(Utils.GetLoggerFactory);
    }

    // [Fact]
    public async Task Initialize_WhenFailWriteMessage_ShouldRetryInitializeAndReadThenCommitMessage()
    {
        var tcs = new TaskCompletionSource<bool>();

        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .ThrowsAsync(new Driver.TransportException(new RpcException(Grpc.Core.Status.DefaultCancelled)))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask);

        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .Returns(new ValueTask<bool>(true))
            .Returns(new ValueTask<bool>(true))
            .Returns(new ValueTask<bool>(true))
            .Returns(new ValueTask<bool>(true))
            .Returns(new ValueTask<bool>(tcs.Task));

        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(new FromServer
            {
                Status = StatusIds.Types.StatusCode.Success,
                InitResponse = new StreamReadMessage.Types.InitResponse { SessionId = "SessionId" }
            })
            .Returns(
                new FromServer
                {
                    Status = StatusIds.Types.StatusCode.Success,
                    StartPartitionSessionRequest = new StreamReadMessage.Types.StartPartitionSessionRequest
                    {
                        CommittedOffset = 0,
                        PartitionOffsets = new OffsetsRange { End = 0, Start = 0 },
                        PartitionSession = new StreamReadMessage.Types.PartitionSession
                            { Path = "/topic", PartitionId = 1, PartitionSessionId = 1 }
                    }
                })
            .Returns(
                new FromServer
                {
                    ReadResponse = new StreamReadMessage.Types.ReadResponse
                    {
                        BytesSize = 50, PartitionData =
                        {
                            new StreamReadMessage.Types.ReadResponse.Types.PartitionData
                            {
                                Batches =
                                {
                                    new StreamReadMessage.Types.ReadResponse.Types.Batch
                                    {
                                        MessageData =
                                        {
                                            new StreamReadMessage.Types.ReadResponse.Types.MessageData
                                                { Data = ByteString.CopyFrom(BitConverter.GetBytes(100)) }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            )
            .Returns(
                new FromServer
                {
                    CommitOffsetResponse =
                        new StreamReadMessage.Types.CommitOffsetResponse
                        {
                            PartitionsCommittedOffsets =
                            {
                                new StreamReadMessage.Types.CommitOffsetResponse.Types.PartitionCommittedOffset
                                {
                                    PartitionSessionId = 1,
                                    CommittedOffset = 50
                                }
                            }
                        }
                }
            );


        using var reader = new ReaderBuilder<int>(_mockIDriver.Object)
        {
            ConsumerName = "Consumer Tester",
            MemoryUsageMaxBytes = 200,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        var message = await reader.ReadAsync();
        // await message.CommitAsync();
        // Assert.Equal(100, message.Data);
    }
}
