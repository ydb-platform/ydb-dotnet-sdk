using Grpc.Core;
using Moq;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using Xunit;
using Ydb.Issue;
using Ydb.Sdk.Ado;
using Ydb.Sdk.OpenTelemetry;
using Ydb.Sdk.Topic.Writer;
using Ydb.Topic;
using Range = Moq.Range;

namespace Ydb.Sdk.Topic.Tests;

using WriterStream = IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>;
using FromClient = StreamWriteMessage.Types.FromClient;

public class WriterMetricsReporterTests
{
    private readonly IDriverFactoryMock _driverFactoryMock;
    private readonly Mock<WriterStream> _mockStream = new();
    private readonly Task<bool> _lastMoveNext;

    public WriterMetricsReporterTests()
    {
        var driver = new Mock<IDriver>();
        driver.Setup(mock => mock.BidirectionalStreamCall(
                It.IsAny<Method<FromClient, StreamWriteMessage.Types.FromServer>>(),
                It.IsAny<GrpcRequestSettings>()))
            .ReturnsAsync(_mockStream.Object);
        driver.Setup(mock => mock.LoggerFactory).Returns(Utils.LoggerFactory);
        driver.Setup(mock => mock.DisposeAsync())
            .Callback(() => driver.Setup(mock => mock.IsDisposed).Returns(true));
        _driverFactoryMock = new IDriverFactoryMock(driver, "Writer_Metrics_Mock");

        var lastMoveNext = new TaskCompletionSource<bool>();
        _lastMoveNext = lastMoveNext.Task;
        _mockStream.Setup(stream => stream.RequestStreamComplete()).Returns(() =>
        {
            lastMoveNext.TrySetResult(false);
            return Task.CompletedTask;
        });
    }

    private sealed class FailSerializer : ISerializer<int>
    {
        public byte[] Serialize(int data) => throw new Exception("Some serialize exception");
    }

    [Fact]
    public async Task WrittenMessages_DoesNotCountSerializationFailure()
    {
        const string topic = "/writer-serialization-metrics";
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);
        await using var writer = new WriterBuilder<int>(_driverFactoryMock, topic)
        { Serializer = new FailSerializer() }.Build();
        _mockStream.Setup(stream => stream.Write(It.IsAny<FromClient>())).Returns(Task.CompletedTask);
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .Returns(_lastMoveNext);

        await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync(123));

        AssertWrittenMessages(meterProvider, exportedItems, topic);
    }

    [Fact]
    public async Task WrittenMessages_CountsBufferedMessagesCompletedDuringReconnect()
    {
        const string topic = "/writer-reconnect-metrics";
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);
        var writeTcs1 = new TaskCompletionSource();
        var writeTcs2 = new TaskCompletionSource();
        var writeTcs3 = new TaskCompletionSource();
        var moveTcs = new TaskCompletionSource<bool>();
        var moveTcsRetry = new TaskCompletionSource<bool>();

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
            .Returns(() =>
            {
                moveTcsRetry.SetResult(true);
                return Task.CompletedTask;
            });
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .Returns(moveTcs.Task)
            .ReturnsAsync(true)
            .Returns(moveTcsRetry.Task)
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
        await using var writer = new WriterBuilder<long>(_driverFactoryMock, topic).Build();

        using var cancellation = new CancellationTokenSource();
        var cancelledWrite = writer.WriteAsync(100L, cancellation.Token);
        await writeTcs1.Task;
        await cancellation.CancelAsync();
        var alreadyWritten = writer.WriteAsync(100L);
        await writeTcs2.Task;
        var written = writer.WriteAsync(100L);
        await writeTcs3.Task;
        moveTcs.SetResult(false);

        await Assert.ThrowsAsync<TaskCanceledException>(() => cancelledWrite);
        Assert.Equal(PersistenceStatus.AlreadyWritten, (await alreadyWritten).Status);
        Assert.Equal(PersistenceStatus.Written, (await written).Status);

        var writtenMessages = AssertWrittenMessages(meterProvider, exportedItems, topic,
            ("written", 1), ("already_written", 2));
        AssertGeneratedWriterName(Assert.Single(writtenMessages.Select(item => item.WriterName).Distinct()));

        _mockStream.Verify(stream => stream.Write(It.IsAny<FromClient>()), Times.Exactly(6));
        _mockStream.Verify(stream => stream.MoveNextAsync(), Times.Between(4, 5, Range.Inclusive));
        _mockStream.Verify(stream => stream.Current, Times.Exactly(3));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task WrittenMessages_CountsAcknowledgementsOnce_EvenAfterCancellation(bool skipped, bool cancel)
    {
        const string topic = "/writer-ack-metrics";
        const string customWriterName = "shared-ack-writer";
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);
        var writeSent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ackReady = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ackHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                writeSent.TrySetResult();
                return Task.CompletedTask;
            });
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .Returns(ackReady.Task)
            .Returns(() =>
            {
                ackHandled.TrySetResult();
                return _lastMoveNext;
            });

        var ack = new StreamWriteMessage.Types.WriteResponse.Types.WriteAck { SeqNo = 1 };
        if (skipped)
        {
            ack.Skipped = new StreamWriteMessage.Types.WriteResponse.Types.WriteAck.Types.Skipped();
        }
        else
        {
            ack.Written = new StreamWriteMessage.Types.WriteResponse.Types.WriteAck.Types.Written();
        }

        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                Status = StatusIds.Types.StatusCode.Success,
                InitResponse = new StreamWriteMessage.Types.InitResponse { SessionId = "metrics-session" }
            })
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                Status = StatusIds.Types.StatusCode.Success,
                WriteResponse = new StreamWriteMessage.Types.WriteResponse { Acks = { ack, ack.Clone() } }
            });

        await using var writer = new WriterBuilder<int>(_driverFactoryMock, topic)
        {
            WriterName = skipped ? customWriterName : null
        }.Build();
        using var cancellation = new CancellationTokenSource();
        var writeTask = writer.WriteAsync(123, cancellation.Token);
        await writeSent.Task.WaitAsync(TimeSpan.FromSeconds(5));
        AssertWrittenMessages(meterProvider, exportedItems, topic);

        if (cancel)
        {
            await cancellation.CancelAsync();
            await Assert.ThrowsAsync<TaskCanceledException>(() => writeTask);
        }

        ackReady.SetResult(true);
        await ackHandled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        if (!cancel)
        {
            Assert.Equal(skipped ? PersistenceStatus.AlreadyWritten : PersistenceStatus.Written,
                (await writeTask).Status);
        }

        await writer.DisposeAsync();
        var writtenMessages = AssertWrittenMessages(meterProvider, exportedItems, topic,
            (skipped ? "already_written" : "written", 1));
        var writerName = Assert.Single(writtenMessages).WriterName;
        if (skipped)
        {
            Assert.Equal(customWriterName, writerName);
        }
        else
        {
            AssertGeneratedWriterName(writerName);
        }

        _mockStream.Verify(stream => stream.Write(It.Is<FromClient>(message =>
            message.InitRequest != null && message.InitRequest.ProducerId == "")), Times.Once);
    }

    [Fact]
    public void WriterName_GeneratesNameForNullAndGroupsRepeatedCustomName()
    {
        const string topic = "/writer-name-metrics";
        const string sharedWriterName = "shared-writer";
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);
        var metrics = new[]
        {
            new WriterMetricsReporter("localhost:2136", "/local", topic, null),
            new WriterMetricsReporter("localhost:2136", "/local", topic, ""),
            new WriterMetricsReporter("localhost:2136", "/local", topic, " "),
            new WriterMetricsReporter("localhost:2136", "/local", topic, sharedWriterName),
            new WriterMetricsReporter("localhost:2136", "/local", topic, sharedWriterName)
        };
        foreach (var metric in metrics)
        {
            metric.ReportWritten(PersistenceStatus.Written);
        }

        var writtenMessages = AssertWrittenMessages(meterProvider, exportedItems, topic,
            ("written", 1), ("written", 1), ("written", 1), ("written", 2));
        var byWriterName = writtenMessages.ToDictionary(item => item.WriterName, item => item.Count);
        Assert.Equal(1, byWriterName[""]);
        Assert.Equal(1, byWriterName[" "]);
        Assert.Equal(2, byWriterName[sharedWriterName]);
        AssertGeneratedWriterName(Assert.Single(byWriterName.Keys,
            writerName => writerName != "" && writerName != " " && writerName != sharedWriterName));
    }

    private static MeterProvider CreateMeterProvider(List<Metric> exportedItems) =>
        global::OpenTelemetry.Sdk.CreateMeterProviderBuilder()
            .AddYdbTopic()
            .AddInMemoryExporter(exportedItems)
            .Build();

    private static List<(string WriterName, string Status, long Count)> AssertWrittenMessages(
        MeterProvider meterProvider,
        List<Metric> exportedItems,
        string topic,
        params (string Status, long Count)[] expected)
    {
        exportedItems.Clear();
        Assert.True(meterProvider.ForceFlush());
        var actual = new List<(string WriterName, string Status, long Count)>();
        foreach (var metric in exportedItems.Where(metric => metric.Name == "ydb.topic.writer.written.messages"))
        {
            Assert.Equal(MetricType.LongSum, metric.MetricType);
            Assert.Equal("{message}", metric.Unit);
            foreach (var point in metric.GetMetricPoints())
            {
                var tags = ToDictionary(point.Tags);
                if (!Equals(topic, tags.GetValueOrDefault("topic")))
                {
                    continue;
                }

                Assert.Equal(5, tags.Count);
                Assert.Equal("localhost:2136", tags["endpoint"]);
                Assert.Equal("/local", tags["database"]);
                actual.Add((
                    Assert.IsType<string>(tags["writer.name"]),
                    Assert.IsType<string>(tags["status"]),
                    point.GetSumLong()));
            }
        }

        Assert.Equal(
            expected.OrderBy(item => item.Status).ThenBy(item => item.Count),
            actual.Select(item => (item.Status, item.Count)).OrderBy(item => item.Status).ThenBy(item => item.Count));
        return actual;
    }

    private static void AssertGeneratedWriterName(string writerName)
    {
        const string prefix = "writer-";
        Assert.StartsWith(prefix, writerName);
        Assert.True(long.TryParse(writerName.AsSpan(prefix.Length), out var id));
        Assert.True(id > 0);
    }

    private static Dictionary<string, object?> ToDictionary(ReadOnlyTagCollection tags)
    {
        var dictionary = new Dictionary<string, object?>();
        foreach (var tag in tags)
        {
            dictionary[tag.Key] = tag.Value;
        }

        return dictionary;
    }
}
