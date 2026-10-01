using System.Diagnostics;
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

[Collection("Topic metrics")]
public class WriterMetricsReporterTests
{
    private sealed class BufferMetricsSource : IWriterMetricsSource
    {
        public long BufferUsed { get; set; }

        public long BufferLimit { get; set; }

        public long OldestMessageTimestamp { get; set; }
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
    public void BufferLimit_ReportsEachWriterAndRemovesClosedContribution()
    {
        const string topic = "/writer-buffer-limit";
        const string metricName = "ydb.topic.writer.buffer.limit.bytes";
        using (new WriterMetricsReporter("localhost:2136", "/local", topic, "second-limit",
                   new BufferMetricsSource { BufferLimit = 200 }))
        {
            using (new WriterMetricsReporter("localhost:2136", "/local", topic, "first-limit",
                       new BufferMetricsSource { BufferLimit = 100 }))
            {
                var values = Collect();
                Assert.Equal(2, values.Count);
                Assert.Equal(100, values["first-limit"]);
                Assert.Equal(200, values["second-limit"]);
            }

            var remainingValues = Collect();
            Assert.Single(remainingValues);
            Assert.Equal(200, remainingValues["second-limit"]);
        }

        Assert.Empty(Collect());

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

            return GetPoints(exportedItems, metricName, topic).ToDictionary(
                point => (string)GetTags(point)["writer.name"]!, point => point.GetGaugeLastValueLong());
        }
    }

    [Fact]
    public void SendingOldestAge_ReportsEachWriterAndRemovesClosedContribution()
    {
        const string topic = "/writer-oldest-age";
        const string metricName = "ydb.topic.writer.sending.oldest_age";
        var now = Stopwatch.GetTimestamp();
        var firstSource = new BufferMetricsSource { OldestMessageTimestamp = now - 2 * Stopwatch.Frequency };
        var secondSource = new BufferMetricsSource { OldestMessageTimestamp = now - Stopwatch.Frequency };
        using (new WriterMetricsReporter("localhost:2136", "/local", topic, "first", firstSource))
        {
            using (new WriterMetricsReporter("localhost:2136", "/local", topic, "second", secondSource))
            {
                var values = Collect();
                Assert.Equal(2, values.Count);
                Assert.True(values["first"] >= 2);
                Assert.True(values["second"] >= 1);
                firstSource.OldestMessageTimestamp = 0;
                Assert.Equal(0, Collect()["first"]);
            }

            Assert.Equal(0, Assert.Single(Collect()).Value);
        }

        Assert.Empty(Collect());

        Dictionary<string, double> Collect()
        {
            var exportedItems = new List<Metric>();
            using var provider = CreateMeterProvider(exportedItems);
            Assert.True(provider.ForceFlush());
            foreach (var metric in exportedItems.Where(item => item.Name == metricName))
            {
                Assert.Equal(MetricType.DoubleGauge, metric.MetricType);
                Assert.Equal("s", metric.Unit);
            }

            return GetPoints(exportedItems, metricName, topic).ToDictionary(point =>
            {
                var tags = GetTags(point);
                Assert.Equal(4, tags.Count);
                Assert.Equal("localhost:2136", tags["endpoint"]);
                Assert.Equal("/local", tags["database"]);
                Assert.Equal(topic, tags["topic"]);
                return (string)tags["writer.name"]!;
            }, point => point.GetGaugeLastValueDouble());
        }
    }

    [Fact]
    public void SendingOldestAge_ReportsMessagesAcceptedBeforeSubscription()
    {
        const string topic = "/writer-late-subscription";
        var message = new MessageSending(
            new StreamWriteMessage.Types.WriteRequest.Types.MessageData(),
            new TaskCompletionSource<WriteResult>(), default);
        using var metrics = new WriterMetricsReporter("localhost:2136", "/local", topic, "writer",
            new BufferMetricsSource { OldestMessageTimestamp = message.SendTimestamp });

        Assert.True(CollectOldestAge(topic) is > 0);
    }

    [Fact]
    public async Task BufferUsed_FollowsLimiterReservationAndRelease()
    {
        const string topic = "/writer-buffer-used";
        const string metricName = "ydb.topic.writer.buffer.used.bytes";

        var sent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ageAtFirstSend = 0d;
        var closed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var stream = new Mock<WriterStream>();
        stream.Setup(instance => instance.Write(It.IsAny<FromClient>())).Returns<FromClient>(message =>
        {
            if (message.WriteRequest != null)
            {
                ageAtFirstSend = CollectOldestAge(topic) ?? 0;
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
            Assert.Equal(0, CollectOldestAge(topic));
            using var cancellation = new CancellationTokenSource();
            var write = writer.WriteAsync(100L, cancellation.Token);
            await sent.Task.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.True(ageAtFirstSend > 0);
            Assert.Equal(8, Collect());
            Assert.Equal(8, Collect());
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => write);
            Assert.Equal(0, Collect());
            Assert.True(CollectOldestAge(topic) is > 0);
        }
        finally
        {
            await writer.DisposeAsync();
        }

        Assert.Null(Collect());
        Assert.Null(CollectOldestAge(topic));

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
            var limits = GetPoints(exportedItems, "ydb.topic.writer.buffer.limit.bytes", topic);
            Assert.Equal(points.Count, limits.Count);
            if (limits.Count != 0)
            {
                Assert.Equal(1, Assert.Single(limits).GetGaugeLastValueLong());
            }

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
            Assert.Equal(0, CollectOldestAge(topic));
            var message = new Message<byte[]>([1]);
            message.Metadata.Add(new Metadata("key", new byte[20]));
            accepted = writer.WriteAsync(message);
            Assert.True(CollectOldestAge(topic) is > 0);
            using var cancellation = new CancellationTokenSource();
            var rejected = writer.WriteAsync([2], cancellation.Token);
            Assert.False(rejected.IsCompleted);
            await cancellation.CancelAsync();
            Assert.Equal("Buffer overflow", (await Assert.ThrowsAsync<WriterException>(() => rejected)).Message);
            Assert.True(CollectOldestAge(topic) is > 0);
            opening.TrySetCanceled();
        }

        await Assert.ThrowsAsync<WriterException>(() => accepted);
        Assert.Null(CollectOldestAge(topic));
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
    public async Task WrittenMessages_ReportsTwoMessagesWithRetryAndCanceledWait()
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
        var acknowledgementsProcessed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var retryAge = 0d;
        var retryTokenRequested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var retryToken = new TaskCompletionSource<string?>(TaskCreationOptions.RunContinuationsAsynchronously);
        stream.Setup(instance => instance.AuthToken()).Returns(() =>
        {
            if (!reconnect.Task.IsCompleted)
            {
                return new ValueTask<string?>((string?)null);
            }

            retryTokenRequested.TrySetResult();
            return new ValueTask<string?>(retryToken.Task);
        });
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
                retryAge = CollectOldestAge(topic) ?? 0;
                retriedWriteSent.SetResult(true);
                return Task.CompletedTask;
            });
        stream.SetupSequence(instance => instance.MoveNextAsync())
            .ReturnsAsync(true)
            .Returns(reconnect.Task)
            .ReturnsAsync(true)
            .Returns(retriedWriteSent.Task)
            .Returns(() =>
            {
                acknowledgementsProcessed.TrySetResult();
                return closed.Task;
            });
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
            var source = Assert.IsAssignableFrom<IWriterMetricsSource>(writer);
            Assert.Equal(0, CollectOldestAge(topic));
            using var cancellation = new CancellationTokenSource();
            var alreadyWritten = writer.WriteAsync(100L, cancellation.Token);
            await firstWriteSent.Task.WaitAsync(TimeSpan.FromSeconds(5));
            var firstTimestamp = source.OldestMessageTimestamp;
            Assert.NotEqual(0, firstTimestamp);
            Assert.True(CollectOldestAge(topic) is > 0);
            await cancellation.CancelAsync();
            await Assert.ThrowsAsync<TaskCanceledException>(() => alreadyWritten);
            Assert.Equal(firstTimestamp, source.OldestMessageTimestamp);
            Assert.True(CollectOldestAge(topic) is > 0);
            var written = writer.WriteAsync(200L);
            await secondWriteSent.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(firstTimestamp, source.OldestMessageTimestamp);
            reconnect.SetResult(true);

            try
            {
                await retryTokenRequested.Task.WaitAsync(TimeSpan.FromSeconds(5));
                Assert.True(CollectOldestAge(topic) is > 0);
            }
            finally
            {
                retryToken.TrySetResult(null);
            }

            Assert.Equal(PersistenceStatus.Written, (await written.WaitAsync(TimeSpan.FromSeconds(5))).Status);
            await acknowledgementsProcessed.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.True(retryAge > 0);
            Assert.Equal(0, CollectOldestAge(topic));
        }

        Assert.Null(CollectOldestAge(topic));
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
        var ackDuration = GetMetric(exportedItems, "ydb.topic.writer.message.ack.duration");
        Assert.Equal(MetricType.Histogram, ackDuration.MetricType);
        Assert.Equal("s", ackDuration.Unit);
        var durationPoint = GetSinglePoint(ackDuration, topic);
        var durationTags = GetTags(durationPoint);
        Assert.Equal(4, durationTags.Count);
        AssertCommonTags(durationTags, topic);
        Assert.Equal(2, durationPoint.GetHistogramCount());
        Assert.True(durationPoint.GetHistogramSum() >= 0);
        var boundaries = new List<double>();
        foreach (var bucket in durationPoint.GetHistogramBuckets())
        {
            boundaries.Add(bucket.ExplicitBound);
        }

        Assert.Equal([0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, double.PositiveInfinity], boundaries);
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

    [Fact]
    public void MessageAckDuration_DoesNotRecordZeroTimestamp()
    {
        const string topic = "/writer-unsent-ack-duration";
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);
        using var metrics = new WriterMetricsReporter("localhost:2136", "/local", topic, "writer",
            new BufferMetricsSource());

        metrics.ReportMessageAckDuration(0);

        Assert.True(meterProvider.ForceFlush());
        Assert.Empty(GetPoints(exportedItems, "ydb.topic.writer.message.ack.duration", topic));
    }

    private static double? CollectOldestAge(string topic)
    {
        const string metricName = "ydb.topic.writer.sending.oldest_age";
        var exportedItems = new List<Metric>();
        using var provider = CreateMeterProvider(exportedItems);
        Assert.True(provider.ForceFlush());
        var points = GetPoints(exportedItems, metricName, topic);
        if (points.Count == 0)
        {
            return null;
        }

        var metric = GetMetric(exportedItems, metricName);
        Assert.Equal(MetricType.DoubleGauge, metric.MetricType);
        Assert.Equal("s", metric.Unit);
        var point = Assert.Single(points);
        var tags = GetTags(point);
        Assert.Equal(4, tags.Count);
        AssertCommonTags(tags, topic);
        return point.GetGaugeLastValueDouble();
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
