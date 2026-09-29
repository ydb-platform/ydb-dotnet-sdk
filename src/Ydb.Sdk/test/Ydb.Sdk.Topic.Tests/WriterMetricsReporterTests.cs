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
    private sealed class BufferMetricsSource : IWriterMetricsSource
    {
        public long BufferUsed { get; set; }
    }

    [Fact]
    public void WriterName_MustBeUniqueAmongActiveWriters()
    {
        var source = new BufferMetricsSource();
        using (new WriterMetricsReporter("localhost:2136", "/local", "/first", "same-writer", source))
        {
            Assert.Throws<ArgumentException>(() =>
                new WriterMetricsReporter("localhost:2136", "/local", "/second", "same-writer", source));
        }

        using var replacement = new WriterMetricsReporter("localhost:2136", "/local", "/second", "same-writer",
            source);
    }

    [Fact]
    public void BufferUsed_ReportsEachWriterAndRemovesClosedContribution()
    {
        const string topic = "/writer-buffer-by-name";
        const string metricName = "ydb.topic.writer.buffer.used.bytes";

        var firstUsed = new BufferMetricsSource { BufferUsed = 5 };
        var secondUsed = new BufferMetricsSource { BufferUsed = 7 };
        var firstName = new WriterConfig(topic, null, null, Codec.Raw, 1, null).WriterName;
        var secondName = new WriterConfig(topic, null, null, Codec.Raw, 1, null).WriterName;
        Assert.NotEqual(firstName, secondName);
        var first = new WriterMetricsReporter("localhost:2136", "/local", topic, firstName, firstUsed);
        var second = new WriterMetricsReporter("localhost:2136", "/local", topic, secondName, secondUsed);
        try
        {
            var values = Collect();
            Assert.Equal(2, values.Count);
            Assert.Equal(5, values[firstName]);
            Assert.Equal(7, values[secondName]);
            secondUsed.BufferUsed = 3;
            values = Collect();
            Assert.Equal(2, values.Count);
            Assert.Equal(5, values[firstName]);
            Assert.Equal(3, values[secondName]);
            first.Dispose();
            values = Collect();
            Assert.Single(values);
            Assert.Equal(3, values[secondName]);
            second.Dispose();
            Assert.Empty(Collect());
        }
        finally
        {
            first.Dispose();
            second.Dispose();
        }

        Dictionary<string, long> Collect()
        {
            var exportedItems = new List<Metric>();
            using var meterProvider = CreateMeterProvider(exportedItems);
            Assert.True(meterProvider.ForceFlush());
            foreach (var metric in exportedItems.Where(item => item.Name == metricName))
            {
                Assert.Equal(MetricType.LongGauge, metric.MetricType);
                Assert.Equal("By", metric.Unit);
            }

            var values = new Dictionary<string, long>();
            foreach (var point in GetPoints(exportedItems, metricName, topic))
            {
                var tags = GetTags(point);
                Assert.Equal(4, tags.Count);
                Assert.Equal("localhost:2136", tags["endpoint"]);
                Assert.Equal("/local", tags["database"]);
                values.Add((string)tags["writer.name"]!, point.GetGaugeLastValueLong());
            }

            return values;
        }
    }

    [Fact]
    public async Task BufferUsed_FollowsLimiterReservationAndRelease()
    {
        const string topic = "/writer-buffer-used";
        const string metricName = "ydb.topic.writer.buffer.used.bytes";

        var sent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var closed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var stream = new Mock<WriterStream>();
        stream.Setup(instance => instance.Write(It.IsAny<FromClient>())).Returns<FromClient>(message =>
        {
            if (message.WriteRequest != null)
            {
                sent.TrySetResult();
            }

            return Task.CompletedTask;
        });
        stream.SetupSequence(instance => instance.MoveNextAsync())
            .ReturnsAsync(true)
            .Returns(closed.Task);
        stream.Setup(instance => instance.Current).Returns(new StreamWriteMessage.Types.FromServer
        {
            InitResponse = new StreamWriteMessage.Types.InitResponse
                { LastSeqNo = 0, PartitionId = 1, SessionId = "session" },
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
        var writer = new WriterBuilder<long>(new IDriverFactoryMock(driver, "writer-buffer-used"), topic)
        {
            WriterName = "writer",
            BufferMaxSize = 1
        }.Build();
        try
        {
            Assert.Equal(0, Collect());
            using var cancellation = new CancellationTokenSource();
            var write = writer.WriteAsync(100L, cancellation.Token);
            await sent.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(8, Collect());
            Assert.Equal(8, Collect());
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => write);
            Assert.Equal(0, Collect());
        }
        finally
        {
            await writer.DisposeAsync();
        }

        Assert.Null(Collect());

        long? Collect()
        {
            var exportedItems = new List<Metric>();
            using var meterProvider = CreateMeterProvider(exportedItems);
            Assert.True(meterProvider.ForceFlush());
            foreach (var metric in exportedItems.Where(item => item.Name == metricName))
            {
                Assert.Equal(MetricType.LongGauge, metric.MetricType);
                Assert.Equal("By", metric.Unit);
            }

            var points = GetPoints(exportedItems, metricName, topic);
            return points.Count == 0 ? null : Assert.Single(points).GetGaugeLastValueLong();
        }
    }

    [Fact]
    public async Task SendingMetrics_DoNotCountBufferOverflow()
    {
        const string topic = "/writer-sending-metrics";
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);
        var opening = new TaskCompletionSource<WriterStream>(TaskCreationOptions.RunContinuationsAsynchronously);
        var driver = new Mock<IDriver>();
        driver.Setup(instance => instance.BidirectionalStreamCall(
                It.IsAny<Method<FromClient, StreamWriteMessage.Types.FromServer>>(),
                It.IsAny<GrpcRequestSettings>()))
            .Returns(new ValueTask<WriterStream>(opening.Task));
        driver.Setup(instance => instance.LoggerFactory).Returns(Utils.LoggerFactory);
        driver.Setup(instance => instance.DisposeAsync())
            .Callback(() => driver.Setup(instance => instance.IsDisposed).Returns(true));
        var writer =
            new WriterBuilder<byte[]>(new IDriverFactoryMock(driver, "writer-sending-metrics"), topic)
            {
                WriterName = "writer",
                BufferMaxSize = 1
            }.Build();
        Task<WriteResult> accepted;
        await using (writer)
        {
            var message = new Message<byte[]>([1]);
            message.Metadata.Add(new Metadata("key", new byte[20]));
            accepted = writer.WriteAsync(message);
            using var cancellation = new CancellationTokenSource();
            var rejected = writer.WriteAsync([2], cancellation.Token);
            Assert.False(rejected.IsCompleted);
            await cancellation.CancelAsync();
            Assert.Equal("Buffer overflow", (await Assert.ThrowsAsync<WriterException>(() => rejected)).Message);
            opening.TrySetCanceled();
        }

        await Assert.ThrowsAsync<WriterException>(() => accepted);
        Assert.True(meterProvider.ForceFlush());
        var metric = GetMetric(exportedItems, "ydb.topic.writer.sending.messages");
        Assert.Equal(MetricType.LongSum, metric.MetricType);
        Assert.Equal("{message}", metric.Unit);
        var point = GetSinglePoint(metric, topic);
        var tags = GetTags(point);
        Assert.Equal(1, point.GetSumLong());
        Assert.Equal(4, tags.Count);
        AssertCommonTags(tags, topic);

        var sendingBytes = GetMetric(exportedItems, "ydb.topic.writer.sending.bytes");
        Assert.Equal(MetricType.LongSum, sendingBytes.MetricType);
        Assert.Equal("By", sendingBytes.Unit);
        var bytesPoint = GetSinglePoint(sendingBytes, topic);
        var bytesTags = GetTags(bytesPoint);
        Assert.Equal(1, bytesPoint.GetSumLong());
        Assert.Equal(4, bytesTags.Count);
        AssertCommonTags(bytesTags, topic);
    }

    [Fact]
    public async Task WrittenMessages_ReportsTwoMessagesWithRetry()
    {
        const string topic = "/writer-metrics";
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);
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
                Status = StatusIds.Types.StatusCode.BadSession
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
        var driver = CreateDriver(stream);
        var writer = new WriterBuilder<long>(new IDriverFactoryMock(driver, "writer-metrics"), topic)
        {
            WriterName = "writer"
        }.Build();

        await using (writer)
        {
            var alreadyWritten = writer.WriteAsync(100L);
            await firstWriteSent.Task;
            var written = writer.WriteAsync(200L);
            await secondWriteSent.Task;
            reconnect.SetResult(true);

            Assert.Equal(PersistenceStatus.AlreadyWritten, (await alreadyWritten).Status);
            Assert.Equal(PersistenceStatus.Written, (await written).Status);
        }

        Assert.True(meterProvider.ForceFlush());

        var metric = GetMetric(exportedItems, "ydb.topic.writer.written.messages");
        Assert.Equal(MetricType.LongSum, metric.MetricType);
        Assert.Equal("{message}", metric.Unit);
        var writtenPoint = GetSinglePoint(metric, topic);
        var writtenTags = GetTags(writtenPoint);
        Assert.Equal(4, writtenTags.Count);
        AssertCommonTags(writtenTags, topic);
        Assert.False(writtenTags.ContainsKey("status"));
        Assert.Equal(2, writtenPoint.GetSumLong());
        var sendingBytes = GetMetric(exportedItems, "ydb.topic.writer.sending.bytes");
        Assert.Equal(MetricType.LongSum, sendingBytes.MetricType);
        Assert.Equal("By", sendingBytes.Unit);
        var bytesPoint = GetSinglePoint(sendingBytes, topic);
        var bytesTags = GetTags(bytesPoint);
        Assert.Equal(4, bytesTags.Count);
        AssertCommonTags(bytesTags, topic);
        Assert.Equal(16, bytesPoint.GetSumLong());
        var sessionErrors = GetMetric(exportedItems, "ydb.topic.writer.session.errors");
        Assert.Equal(MetricType.LongSum, sessionErrors.MetricType);
        Assert.Equal("{error}", sessionErrors.Unit);
        var errorPoint = GetSinglePoint(sessionErrors, topic);
        var errorTags = GetTags(errorPoint);
        Assert.Equal(7, errorTags.Count);
        AssertCommonTags(errorTags, topic);
        Assert.Equal("retry", errorTags["retry_decision"]);
        Assert.Equal("BadSession", errorTags["status_code"]);
        Assert.Equal("ydb_error", errorTags["error.type"]);
        Assert.Equal(1, errorPoint.GetSumLong());
        stream.Verify(instance => instance.Write(It.Is<FromClient>(message =>
            message.WriteRequest != null && message.WriteRequest.Messages[0].SeqNo == 1)), Times.Once);
        stream.Verify(instance => instance.Write(It.Is<FromClient>(message =>
            message.WriteRequest != null && message.WriteRequest.Messages[0].SeqNo == 2)), Times.Exactly(2));
    }

    [Fact]
    public async Task SessionErrors_RecordsStopForNonRetryableInitResponse()
    {
        const string topic = "/writer-session-stop";
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);
        var stream = new Mock<WriterStream>();
        stream.Setup(instance => instance.Write(It.IsAny<FromClient>())).Returns(Task.CompletedTask);
        stream.Setup(instance => instance.MoveNextAsync()).ReturnsAsync(true);
        stream.Setup(instance => instance.Current).Returns(new StreamWriteMessage.Types.FromServer
        {
            Status = StatusIds.Types.StatusCode.Unauthorized
        });
        var driver = CreateDriver(stream);
        var writer = new WriterBuilder<long>(new IDriverFactoryMock(driver, "writer-stop"), topic)
        {
            WriterName = "writer"
        }.Build();

        await using (writer)
        {
            await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync(100L).WaitAsync(TimeSpan.FromSeconds(5)));
        }

        Assert.True(meterProvider.ForceFlush());

        var metric = GetMetric(exportedItems, "ydb.topic.writer.session.errors");
        var point = GetSinglePoint(metric, topic);
        var tags = GetTags(point);
        Assert.Equal("stop", tags["retry_decision"]);
        Assert.Equal("Unauthorized", tags["status_code"]);
        Assert.Equal("ydb_error", tags["error.type"]);
        Assert.Equal(1, point.GetSumLong());
        driver.Verify(instance => instance.BidirectionalStreamCall(
            It.IsAny<Method<FromClient, StreamWriteMessage.Types.FromServer>>(),
            It.IsAny<GrpcRequestSettings>()), Times.Once);
    }

    [Fact]
    public async Task SessionErrors_DoesNotRecordSerializationFailureOrNormalClose()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);
        var stream = new Mock<WriterStream>();
        var closed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var active = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        stream.Setup(instance => instance.Write(It.IsAny<FromClient>())).Returns(Task.CompletedTask);
        stream.SetupSequence(instance => instance.MoveNextAsync())
            .ReturnsAsync(true)
            .Returns(() =>
            {
                active.SetResult();
                return closed.Task;
            });
        stream.Setup(instance => instance.Current).Returns(new StreamWriteMessage.Types.FromServer
        {
            InitResponse = new StreamWriteMessage.Types.InitResponse { SessionId = "session" },
            Status = StatusIds.Types.StatusCode.Success
        });
        stream.Setup(instance => instance.RequestStreamComplete()).Returns(() =>
        {
            closed.TrySetResult(false);
            return Task.CompletedTask;
        });
        var driver = CreateDriver(stream);
        var serializer = new Mock<ISerializer<long>>();
        serializer.Setup(instance => instance.Serialize(It.IsAny<long>())).Throws(new InvalidOperationException());
        var writer = new WriterBuilder<long>(new IDriverFactoryMock(driver, "writer-serialization"),
            "/writer-serialization")
        {
            WriterName = "writer",
            Serializer = serializer.Object
        }.Build();

        await using (writer)
        {
            await Assert.ThrowsAsync<WriterException>(() => writer.WriteAsync(100L));
            await active.Task.WaitAsync(TimeSpan.FromSeconds(5));
        }

        Assert.True(meterProvider.ForceFlush());
        foreach (var metric in exportedItems.Where(item => item.Name == "ydb.topic.writer.session.errors"))
        {
            foreach (var point in metric.GetMetricPoints())
            {
                Assert.NotEqual("/writer-serialization", GetTags(point)["topic"]);
            }
        }

        driver.Verify(instance => instance.BidirectionalStreamCall(
            It.IsAny<Method<FromClient, StreamWriteMessage.Types.FromServer>>(),
            It.IsAny<GrpcRequestSettings>()), Times.Once);
    }

    private static MeterProvider CreateMeterProvider(List<Metric> exportedItems) =>
        global::OpenTelemetry.Sdk.CreateMeterProviderBuilder()
            .AddYdbTopic()
            .AddInMemoryExporter(exportedItems)
            .Build();

    private static Mock<IDriver> CreateDriver(Mock<WriterStream> stream)
    {
        var driver = new Mock<IDriver>();
        driver.Setup(instance => instance.BidirectionalStreamCall(
                It.IsAny<Method<FromClient, StreamWriteMessage.Types.FromServer>>(),
                It.IsAny<GrpcRequestSettings>()))
            .ReturnsAsync(stream.Object);
        driver.Setup(instance => instance.LoggerFactory).Returns(Utils.LoggerFactory);
        driver.Setup(instance => instance.DisposeAsync())
            .Callback(() => driver.Setup(instance => instance.IsDisposed).Returns(true));
        return driver;
    }

    private static Metric GetMetric(List<Metric> exportedItems, string name) =>
        Assert.Single(exportedItems, item => item.Name == name);

    private static List<MetricPoint> GetPoints(List<Metric> exportedItems, string name, string topic)
    {
        var points = new List<MetricPoint>();
        foreach (var metric in exportedItems.Where(item => item.Name == name))
        {
            foreach (var point in metric.GetMetricPoints())
            {
                if (Equals(topic, GetTags(point).GetValueOrDefault("topic")))
                {
                    points.Add(point);
                }
            }
        }

        return points;
    }

    private static MetricPoint GetSinglePoint(Metric metric, string topic)
    {
        var points = new List<MetricPoint>();
        foreach (var point in metric.GetMetricPoints())
        {
            if (Equals(topic, GetTags(point)["topic"]))
            {
                points.Add(point);
            }
        }

        return Assert.Single(points);
    }

    private static void AssertCommonTags(Dictionary<string, object?> tags, string topic)
    {
        Assert.Equal("localhost:2136", tags["endpoint"]);
        Assert.Equal("/local", tags["database"]);
        Assert.Equal(topic, tags["topic"]);
        Assert.Equal("writer", tags["writer.name"]);
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
