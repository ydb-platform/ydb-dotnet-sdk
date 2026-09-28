using Grpc.Core;
using Moq;
using OpenTelemetry.Metrics;
using Xunit;
using Ydb.Sdk.OpenTelemetry;
using Ydb.Sdk.Topic.Writer;
using Ydb.Topic;

namespace Ydb.Sdk.Topic.Tests;

using WriterStream = IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>;
using FromClient = StreamWriteMessage.Types.FromClient;

public class WriterMetricsReporterTests
{
    [Fact]
    public async Task SendingMessages_DoesNotCountBufferOverflow()
    {
        const string topic = "/writer-sending-metrics";
        var exportedItems = new List<Metric>();
        using var meterProvider = global::OpenTelemetry.Sdk.CreateMeterProviderBuilder()
            .AddYdbTopic()
            .AddInMemoryExporter(exportedItems)
            .Build();
        var opening = new TaskCompletionSource<WriterStream>(TaskCreationOptions.RunContinuationsAsynchronously);
        var driver = new Mock<IDriver>();
        driver.Setup(instance => instance.BidirectionalStreamCall(
                It.IsAny<Method<FromClient, StreamWriteMessage.Types.FromServer>>(),
                It.IsAny<GrpcRequestSettings>()))
            .Returns(new ValueTask<WriterStream>(opening.Task));
        driver.Setup(instance => instance.LoggerFactory).Returns(Utils.LoggerFactory);
        driver.Setup(instance => instance.DisposeAsync())
            .Callback(() => driver.Setup(instance => instance.IsDisposed).Returns(true));
        await using var writer =
            new WriterBuilder<byte[]>(new IDriverFactoryMock(driver, "writer-sending-metrics"), topic)
            {
                WriterName = "writer",
                BufferMaxSize = 1
            }.Build();

        var accepted = writer.WriteAsync([1]);
        using var cancellation = new CancellationTokenSource();
        var rejected = writer.WriteAsync([2], cancellation.Token);
        Assert.False(rejected.IsCompleted);
        await cancellation.CancelAsync();
        Assert.Equal("Buffer overflow", (await Assert.ThrowsAsync<WriterException>(() => rejected)).Message);

        Assert.True(meterProvider.ForceFlush());
        var metric = Assert.Single(exportedItems, item => item.Name == "ydb.topic.writer.sending.messages");
        Assert.Equal(MetricType.LongSum, metric.MetricType);
        Assert.Equal("{message}", metric.Unit);
        var points = 0;
        foreach (var point in metric.GetMetricPoints())
        {
            points++;
            Assert.Equal(1, point.GetSumLong());
            var tags = GetTags(point);
            Assert.Equal(4, tags.Count);
            Assert.Equal("localhost:2136", tags["endpoint"]);
            Assert.Equal("/local", tags["database"]);
            Assert.Equal(topic, tags["topic"]);
            Assert.Equal("writer", tags["writer.name"]);
        }

        Assert.Equal(1, points);
        opening.TrySetCanceled();
        await writer.DisposeAsync();
        await Assert.ThrowsAsync<WriterException>(() => accepted);
    }

    [Fact]
    public async Task WrittenMessages_ReportsTwoMessagesWithRetry()
    {
        const string topic = "/writer-metrics";
        var exportedItems = new List<Metric>();
        using var meterProvider = global::OpenTelemetry.Sdk.CreateMeterProviderBuilder()
            .AddYdbTopic()
            .AddInMemoryExporter(exportedItems)
            .Build();
        var stream = new Mock<WriterStream>();
        var firstWriteSent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondWriteSent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var retriedWriteSent = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var reconnect = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var closed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        stream.SetupSequence(instance => instance.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                firstWriteSent.SetResult();
                return Task.CompletedTask;
            })
            .Returns(() =>
            {
                secondWriteSent.SetResult();
                return Task.CompletedTask;
            })
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                retriedWriteSent.SetResult(true);
                return Task.CompletedTask;
            });
        stream.SetupSequence(instance => instance.MoveNextAsync())
            .ReturnsAsync(true)
            .Returns(reconnect.Task)
            .ReturnsAsync(true)
            .Returns(retriedWriteSent.Task)
            .Returns(closed.Task);
        stream.SetupSequence(instance => instance.Current)
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                InitResponse = new StreamWriteMessage.Types.InitResponse
                    { LastSeqNo = 0, PartitionId = 1, SessionId = "session-1" },
                Status = StatusIds.Types.StatusCode.Success
            })
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                InitResponse = new StreamWriteMessage.Types.InitResponse
                    { LastSeqNo = 1, PartitionId = 1, SessionId = "session-2" },
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
                            SeqNo = 2,
                            Written = new StreamWriteMessage.Types.WriteResponse.Types.WriteAck.Types.Written()
                        }
                    }
                },
                Status = StatusIds.Types.StatusCode.Success
            });
        stream.Setup(instance => instance.RequestStreamComplete()).Returns(() =>
        {
            closed.TrySetResult(false);
            return Task.CompletedTask;
        });
        var driver = new Mock<IDriver>();
        driver.Setup(instance => instance.BidirectionalStreamCall(
                It.IsAny<Method<FromClient, StreamWriteMessage.Types.FromServer>>(),
                It.IsAny<GrpcRequestSettings>()))
            .ReturnsAsync(stream.Object);
        driver.Setup(instance => instance.LoggerFactory).Returns(Utils.LoggerFactory);
        driver.Setup(instance => instance.DisposeAsync())
            .Callback(() => driver.Setup(instance => instance.IsDisposed).Returns(true));
        await using var writer = new WriterBuilder<long>(new IDriverFactoryMock(driver, "writer-metrics"), topic)
        {
            WriterName = "writer"
        }.Build();

        var alreadyWritten = writer.WriteAsync(100L);
        await firstWriteSent.Task;
        var written = writer.WriteAsync(200L);
        await secondWriteSent.Task;
        reconnect.SetResult(false);

        Assert.Equal(PersistenceStatus.AlreadyWritten, (await alreadyWritten).Status);
        Assert.Equal(PersistenceStatus.Written, (await written).Status);
        Assert.True(meterProvider.ForceFlush());

        var metric = Assert.Single(exportedItems,
            item => item.Name == "ydb.topic.writer.written.messages");
        Assert.Equal(MetricType.LongSum, metric.MetricType);
        Assert.Equal("{message}", metric.Unit);
        var matchingPoints = 0;
        foreach (var point in metric.GetMetricPoints())
        {
            var tags = GetTags(point);
            if (!Equals(topic, tags["topic"]))
            {
                continue;
            }

            matchingPoints++;
            Assert.Equal(4, tags.Count);
            Assert.Equal("localhost:2136", tags["endpoint"]);
            Assert.Equal("/local", tags["database"]);
            Assert.Equal(topic, tags["topic"]);
            Assert.Equal("writer", tags["writer.name"]);
            Assert.False(tags.ContainsKey("status"));
            Assert.Equal(2, point.GetSumLong());
        }

        Assert.Equal(1, matchingPoints);
        stream.Verify(instance => instance.Write(It.Is<FromClient>(message =>
            message.WriteRequest != null && message.WriteRequest.Messages[0].SeqNo == 1)), Times.Once);
        stream.Verify(instance => instance.Write(It.Is<FromClient>(message =>
            message.WriteRequest != null && message.WriteRequest.Messages[0].SeqNo == 2)), Times.Exactly(2));
    }

    private static Dictionary<string, object?> GetTags(MetricPoint point)
    {
        var tags = new Dictionary<string, object?>();
        foreach (var tag in point.Tags)
        {
            tags.Add(tag.Key, tag.Value);
        }

        return tags;
    }
}
