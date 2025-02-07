using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
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

    [Fact]
    public async Task Initialize_WhenFailWriteMessage_ShouldRetryInitializeAndReadThenCommitMessage()
    {
        var tcsMoveNext = new TaskCompletionSource<bool>();
        var tcsCommitMessage = new TaskCompletionSource<bool>();

        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            // Write Throws Exception
            .ThrowsAsync(new Driver.TransportException(new RpcException(Grpc.Core.Status.DefaultCancelled)))
            // Write init
            .Returns(Task.CompletedTask)
            // Write ReadRequest { 200 bytes }
            .Returns(Task.CompletedTask)
            // Write StartSessionPartitionRequest
            .Returns(() =>
            {
                tcsMoveNext.SetResult(true);

                return Task.CompletedTask;
            })
            // Write ReadRequest { 50 bytes }
            .Returns(Task.CompletedTask)
            // Write CommitRequest
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
            .Returns(new ValueTask<bool>(new TaskCompletionSource<bool>().Task));

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
                                                Data = ByteString.CopyFrom(BitConverter.GetBytes(100)),
                                                Offset = 1,
                                                CreatedAt = new Timestamp()
                                            }
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
                    Status = StatusIds.Types.StatusCode.Success,
                    CommitOffsetResponse =
                        new StreamReadMessage.Types.CommitOffsetResponse
                        {
                            PartitionsCommittedOffsets =
                            {
                                new StreamReadMessage.Types.CommitOffsetResponse.Types.PartitionCommittedOffset
                                {
                                    PartitionSessionId = 1,
                                    CommittedOffset = 2
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
        await message.CommitAsync();
        Assert.Equal(100, message.Data);
    }
}
