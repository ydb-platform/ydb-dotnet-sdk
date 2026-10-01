using System.Diagnostics;
using System.Diagnostics.Metrics;
using Grpc.Core;
using Moq;
using OpenTelemetry.Metrics;
using Xunit;
using Ydb.Sdk.Topic.Writer;
using Ydb.Topic;
using static Ydb.Sdk.Topic.Tests.TopicMetricsTestUtils;

namespace Ydb.Sdk.Topic.Tests;

using WriterStream = IBidirectionalStream<StreamWriteMessage.Types.FromClient, StreamWriteMessage.Types.FromServer>;
using FromClient = StreamWriteMessage.Types.FromClient;

[CollectionDefinition("Topic metrics", DisableParallelization = true)]
public class TopicMetricsCollection;

[Collection("Topic metrics")]
public class WriterMetricsReporterTests
{
    private sealed class BufferMetricsSource : IWriterMetricsSource
    {
        public long BufferUsed { get; set; }

        public long BufferLimit { get; init; }

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
    public void WriterGauges_TrackEachWriterAndRemoveClosedContributions()
    {
        const string topic = "/writer-gauges";
        var now = Stopwatch.GetTimestamp();
        var firstSource = new BufferMetricsSource
        {
            BufferUsed = 5, BufferLimit = 100, OldestMessageTimestamp = now - 2 * Stopwatch.Frequency
        };
        var secondSource = new BufferMetricsSource
        {
            BufferUsed = 7, BufferLimit = 200, OldestMessageTimestamp = now - Stopwatch.Frequency
        };
        var firstName = new WriterConfig(topic, null, null, Codec.Raw, 1, null).WriterName;
        var secondName = new WriterConfig(topic, null, null, Codec.Raw, 1, null).WriterName;
        Assert.NotEqual(firstName, secondName);
        using (new WriterMetricsReporter("localhost:2136", "/local", topic, firstName, firstSource))
        {
            using (new WriterMetricsReporter("localhost:2136", "/local", topic, secondName, secondSource))
            {
                AssertGauges((firstName, 5, 100, 2), (secondName, 7, 200, 1));
                secondSource.BufferUsed = 3;
                firstSource.OldestMessageTimestamp = 0;
                AssertGauges((firstName, 5, 100, 0), (secondName, 3, 200, 1));
            }

            AssertGauges((firstName, 5, 100, 0));
        }

        AssertGauges();

        void AssertGauges(params (string Name, long Used, long Limit, double Age)[] expected)
        {
            var exportedItems = new List<Metric>();
            using var provider = CreateMeterProvider(exportedItems);
            Assert.True(provider.ForceFlush());
            string[] names =
            [
                "ydb.topic.writer.buffer.used.bytes",
                "ydb.topic.writer.buffer.limit.bytes",
                "ydb.topic.writer.sending.oldest_age"
            ];
            foreach (var name in names)
            {
                Assert.Equal(expected.Length, GetPoints(exportedItems, name, topic).Count);
                if (expected.Length == 0)
                    continue;

                var metric = GetMetric(exportedItems, name);
                var ageMetric = name == "ydb.topic.writer.sending.oldest_age";
                Assert.Equal(ageMetric ? MetricType.DoubleGauge : MetricType.LongGauge, metric.MetricType);
                Assert.Equal(ageMetric ? "s" : "By", metric.Unit);
            }

            foreach (var (name, used, limit, age) in expected)
            {
                Assert.Equal(used, Point(names[0], name).GetGaugeLastValueLong());
                Assert.Equal(limit, Point(names[1], name).GetGaugeLastValueLong());
                var actualAge = Point(names[2], name).GetGaugeLastValueDouble();
                if (age == 0)
                    Assert.Equal(0, actualAge);
                else
                    Assert.True(actualAge >= age);
            }

            MetricPoint Point(string metricName, string writerName)
            {
                var point = Assert.Single(GetPoints(exportedItems, metricName, topic),
                    point => Equals(GetTags(point)["writer.name"], writerName));
                var tags = GetTags(point);
                Assert.Equal(4, tags.Count);
                Assert.Equal("localhost:2136", tags["endpoint"]);
                Assert.Equal("/local", tags["database"]);
                Assert.Equal(topic, tags["topic"]);
                return point;
            }
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public void MessageSendTimestamp_IsCapturedWhenEitherMetricIsEnabled(bool ackEnabled, bool oldestAgeEnabled)
    {
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument.Meter.Name == "Ydb.Sdk.Topic" &&
                ((ackEnabled && instrument.Name == "ydb.topic.writer.message.ack.duration") ||
                 (oldestAgeEnabled && instrument.Name == "ydb.topic.writer.sending.oldest_age")))
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.Start();
        var message = new MessageSending(
            new StreamWriteMessage.Types.WriteRequest.Types.MessageData(),
            new TaskCompletionSource<WriteResult>(), default);
        Assert.Equal(ackEnabled || oldestAgeEnabled, message.SendTimestamp != 0);
    }

    [Fact]
    public async Task BufferUsed_FollowsLimiterReservationAndRelease()
    {
        const string topic = "/writer-buffer-used";
        const string metricName = "ydb.topic.writer.buffer.used.bytes";
        using var subscription = CreateMeterProvider([]);

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
            Assert.Equal(0, CollectOldestAge(topic));
            using var cancellation = new CancellationTokenSource();
            var rejected = writer.WriteAsync([2], cancellation.Token);
            Assert.False(rejected.IsCompleted);
            await cancellation.CancelAsync();
            Assert.Equal("Buffer overflow", (await Assert.ThrowsAsync<WriterException>(() => rejected)).Message);
            Assert.Equal(0, CollectOldestAge(topic));
            opening.TrySetCanceled();
        }

        await Assert.ThrowsAsync<WriterException>(() => accepted);
        Assert.Null(CollectOldestAge(topic));
        Assert.True(meterProvider.ForceFlush());
        Assert.Empty(GetPoints(exportedItems, "ydb.topic.writer.buffer.wait.duration", topic));
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void BufferWaitTimestamp_IsCapturedOnlyWhenEnabled(bool enabled)
    {
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (enabled && instrument.Meter.Name == "Ydb.Sdk.Topic" &&
                instrument.Name == "ydb.topic.writer.buffer.wait.duration")
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.Start();
        Assert.Equal(enabled, WriterMetricsReporter.ReportBufferWaitStart() != 0);
    }

    [Fact]
    public async Task BufferWaitDuration_ReportsOnceAfterRepeatedWakeups()
    {
        const string topic = "/writer-buffer-wait";
        const string metricName = "ydb.topic.writer.buffer.wait.duration";
        var exportedItems = new List<Metric>();
        using var provider = CreateMeterProvider(exportedItems);
        var firstTwoSent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var thirdSent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstAckHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstAck = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondAck = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var thirdAck = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var closed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var stream = new Mock<WriterStream>();
        stream.Setup(instance => instance.Write(It.IsAny<FromClient>()))
            .Callback<FromClient>(request =>
            {
                var seqNo = request.WriteRequest?.Messages.LastOrDefault()?.SeqNo;
                if (seqNo >= 2)
                {
                    firstTwoSent.TrySetResult();
                }

                if (seqNo == 3)
                {
                    thirdSent.TrySetResult();
                }
            }).Returns(Task.CompletedTask);
        stream.SetupSequence(instance => instance.MoveNextAsync())
            .ReturnsAsync(true).Returns(firstAck.Task)
            .Returns(() =>
            {
                firstAckHandled.TrySetResult();
                return secondAck.Task;
            })
            .Returns(thirdAck.Task).Returns(closed.Task);
        stream.SetupSequence(instance => instance.Current)
            .Returns(new StreamWriteMessage.Types.FromServer
            {
                Status = StatusIds.Types.StatusCode.Success,
                InitResponse = new StreamWriteMessage.Types.InitResponse { SessionId = "buffer-wait" }
            })
            .Returns(Ack(1)).Returns(Ack(2)).Returns(Ack(3));
        stream.Setup(instance => instance.RequestStreamComplete()).Returns(() =>
        {
            firstAck.TrySetResult(false);
            secondAck.TrySetResult(false);
            thirdAck.TrySetResult(false);
            closed.TrySetResult(false);
            return Task.CompletedTask;
        });
        var driver = CreateDriver(stream);
        var factory = new IDriverFactoryMock(driver, "writer-buffer-wait");
        using var cancellation = new CancellationTokenSource();
        await using var writer = new WriterBuilder<byte[]>(factory, topic)
        {
            WriterName = "writer",
            BufferMaxSize = 2
        }.Build();
        var first = writer.WriteAsync([1], cancellation.Token);
        var second = writer.WriteAsync([2], cancellation.Token);
        var third = writer.WriteAsync([3, 3]);
        try
        {
            await firstTwoSent.Task.WaitAsync(TimeSpan.FromSeconds(5));
            exportedItems.Clear();
            Assert.True(provider.ForceFlush());
            Assert.Empty(GetPoints(exportedItems, metricName, topic));
            Assert.Equal(2, GetSinglePoint(GetMetric(exportedItems,
                "ydb.topic.writer.sending.messages"), topic).GetSumLong());
            Assert.Equal(2, GetSinglePoint(GetMetric(exportedItems,
                "ydb.topic.writer.buffer.used.bytes"), topic).GetGaugeLastValueLong());
            firstAck.SetResult(true);
            await first.WaitAsync(TimeSpan.FromSeconds(5));
            await firstAckHandled.Task.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.False(third.IsCompleted);
            exportedItems.Clear();
            Assert.True(provider.ForceFlush());
            Assert.Empty(GetPoints(exportedItems, metricName, topic));
            Assert.Equal(1, GetSinglePoint(GetMetric(exportedItems,
                "ydb.topic.writer.buffer.used.bytes"), topic).GetGaugeLastValueLong());
            secondAck.SetResult(true);
            await second.WaitAsync(TimeSpan.FromSeconds(5));
            await thirdSent.Task.WaitAsync(TimeSpan.FromSeconds(5));
            exportedItems.Clear();
            Assert.True(provider.ForceFlush());
            var metric = GetMetric(exportedItems, metricName);
            Assert.Equal(MetricType.Histogram, metric.MetricType);
            Assert.Equal("s", metric.Unit);
            var point = GetSinglePoint(metric, topic);
            Assert.Equal(1, point.GetHistogramCount());
            Assert.True(point.GetHistogramSum() > 0);
            var tags = GetTags(point);
            Assert.Equal(4, tags.Count);
            AssertCommonTags(tags, topic);
            Assert.Equal(3, GetSinglePoint(GetMetric(exportedItems,
                "ydb.topic.writer.sending.messages"), topic).GetSumLong());
            Assert.Equal(4, GetSinglePoint(GetMetric(exportedItems,
                "ydb.topic.writer.sending.bytes"), topic).GetSumLong());
            thirdAck.SetResult(true);
            await third.WaitAsync(TimeSpan.FromSeconds(5));
        }
        finally
        {
            await cancellation.CancelAsync();
        }

        static StreamWriteMessage.Types.FromServer Ack(long seqNo)
        {
            return new StreamWriteMessage.Types.FromServer
            {
                Status = StatusIds.Types.StatusCode.Success,
                WriteResponse = new StreamWriteMessage.Types.WriteResponse
                {
                    Acks =
                    {
                        new StreamWriteMessage.Types.WriteResponse.Types.WriteAck
                        {
                            SeqNo = seqNo,
                            Written = new StreamWriteMessage.Types.WriteResponse.Types.WriteAck.Types.Written()
                        }
                    }
                }
            };
        }
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

    private static List<MetricPoint> GetPoints(List<Metric> exportedItems, string name, string topic) =>
        TopicMetricsTestUtils.GetPoints(exportedItems, name, "topic", topic).ToList();

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
}
