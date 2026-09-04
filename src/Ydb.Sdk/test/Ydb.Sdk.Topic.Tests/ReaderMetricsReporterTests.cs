using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Threading.Channels;
using Grpc.Core;
using Moq;
using Xunit;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Topic.Reader;
using Ydb.Topic;
using static Ydb.Sdk.Topic.Tests.ReaderTestUtils;

namespace Ydb.Sdk.Topic.Tests;

using ReaderStream = IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>;
using FromClient = StreamReadMessage.Types.FromClient;
using FromServer = StreamReadMessage.Types.FromServer;

public class ReaderMetricsReporterTests
{
    private const string PartitionSessionCountMetricName = "ydb.topic.reader.partition_session.count";

    private static readonly string[] MetricNames =
    [
        "ydb.topic.reader.received.messages",
        "ydb.topic.reader.delivered.messages",
        "ydb.topic.reader.commit.queued",
        "ydb.topic.reader.commit.acknowledged"
    ];

    [Fact]
    public async Task PartitionSessionCount_TracksReaderSessionMapAndUnregistersOnDispose()
    {
        const string driverKey = "Reader_Metrics_Partition_Session_Count";
        var timeout = TimeSpan.FromSeconds(5);
        var measurements = new List<Measurement>();
        Instrument? publishedInstrument = null;
        using var listener = CreatePartitionSessionCountListener(
            measurements,
            "partition-count-consumer",
            instrument => publishedInstrument = instrument);
        var responses = Channel.CreateUnbounded<FromServer>();
        responses.Writer.TryWrite(InitResponse);
        var initialized = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = Channel.CreateUnbounded<long>();
        var stopped = Channel.CreateUnbounded<long>();
        FromServer current = null!;
        var mockStream = new Mock<ReaderStream>();
        mockStream.Setup(stream => stream.MoveNextAsync()).Returns(async () =>
        {
            if (!await responses.Reader.WaitToReadAsync().ConfigureAwait(false))
            {
                return false;
            }

            current = await responses.Reader.ReadAsync().ConfigureAwait(false);
            return true;
        });
        mockStream.Setup(stream => stream.Current).Returns(() => current);
        mockStream.Setup(stream => stream.Write(It.IsAny<FromClient>()))
            .Callback<FromClient>(message =>
            {
                if (message.ReadRequest != null)
                {
                    initialized.TrySetResult();
                }

                if (message.StartPartitionSessionResponse != null)
                {
                    started.Writer.TryWrite(message.StartPartitionSessionResponse.PartitionSessionId);
                }

                if (message.StopPartitionSessionResponse != null)
                {
                    stopped.Writer.TryWrite(message.StopPartitionSessionResponse.PartitionSessionId);
                }
            })
            .Returns(Task.CompletedTask);
        mockStream.Setup(stream => stream.AuthToken()).ReturnsAsync((string?)null);
        mockStream.Setup(stream => stream.RequestStreamComplete()).Returns(() =>
        {
            responses.Writer.TryComplete();
            return Task.CompletedTask;
        });
        var mockDriver = new Mock<IDriver>();
        mockDriver.Setup(driver => driver.RegisterOwner()).Returns(true);
        mockDriver.Setup(driver => driver.BidirectionalStreamCall(
            It.IsAny<Method<FromClient, FromServer>>(),
            It.IsAny<GrpcRequestSettings>())).ReturnsAsync(mockStream.Object);
        mockDriver.Setup(driver => driver.DisposeAsync())
            .Callback(() => mockDriver.Setup(driver => driver.IsDisposed).Returns(true));
        mockDriver.Setup(driver => driver.LoggerFactory).Returns(Utils.LoggerFactory);
        var reader = new ReaderBuilder<string>(new IDriverFactoryMock(
            mockDriver,
            driverKey,
            endpoint: "logical.ydb.test:2136",
            database: "/normalized/database"))
        {
            ConsumerName = "partition-count-consumer",
            ReaderName = "partition-count-reader",
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        try
        {
            await initialized.Task.WaitAsync(timeout);
            AssertPartitionSessionCount(0);

            responses.Writer.TryWrite(StartPartitionSessionRequest(partitionSessionId: 1));
            Assert.Equal(1, await started.Reader.ReadAsync().AsTask().WaitAsync(timeout));
            AssertPartitionSessionCount(1);

            responses.Writer.TryWrite(StartPartitionSessionRequest(partitionSessionId: 2));
            Assert.Equal(2, await started.Reader.ReadAsync().AsTask().WaitAsync(timeout));
            AssertPartitionSessionCount(2);

            responses.Writer.TryWrite(StopPartitionSessionRequest(partitionSessionId: 1));
            Assert.Equal(1, await stopped.Reader.ReadAsync().AsTask().WaitAsync(timeout));
            AssertPartitionSessionCount(1);

            responses.Writer.TryWrite(StopPartitionSessionRequest(partitionSessionId: 2));
            Assert.Equal(2, await stopped.Reader.ReadAsync().AsTask().WaitAsync(timeout));
            AssertPartitionSessionCount(0);
        }
        finally
        {
            await reader.DisposeAsync();
            PoolManager.Drivers.TryRemove(driverKey, out _);
        }

        measurements.Clear();
        listener.RecordObservableInstruments();
        Assert.Empty(measurements);

        Assert.NotNull(publishedInstrument);
        Assert.Equal("{session}", publishedInstrument.Unit);
        return;

        void AssertPartitionSessionCount(long expected)
        {
            measurements.Clear();
            listener.RecordObservableInstruments();
            var measurement = Assert.Single(measurements);
            Assert.Equal(expected, measurement.Value);
            Assert.Collection(measurement.Tags,
                tag => AssertTag(tag, "endpoint", "logical.ydb.test:2136"),
                tag => AssertTag(tag, "database", "/normalized/database"),
                tag => AssertTag(tag, "consumer", "partition-count-consumer"),
                tag => AssertTag(tag, "reader.name", "partition-count-reader"));
        }
    }

    [Fact]
    public async Task PartitionSessionCount_ReportsEachReaderSeparately()
    {
        const string firstDriverKey = "Reader_Metrics_First_Named_Reader";
        const string secondDriverKey = "Reader_Metrics_Second_Named_Reader";
        var measurements = new List<Measurement>();
        using var listener = CreatePartitionSessionCountListener(measurements, "shared-consumer");
        var first = CreateReader(firstDriverKey, "first-reader", partitionSessionCount: 1);
        var second = CreateReader(secondDriverKey, "second-reader", partitionSessionCount: 2);

        try
        {
            await Task.WhenAll(first.Started, second.Started).WaitAsync(TimeSpan.FromSeconds(5));
            listener.RecordObservableInstruments();

            Assert.Collection(measurements,
                measurement => AssertReaderMeasurement(measurement, 1, "first-reader"),
                measurement => AssertReaderMeasurement(measurement, 2, "second-reader"));
        }
        finally
        {
            await second.Reader.DisposeAsync();
            await first.Reader.DisposeAsync();
            PoolManager.Drivers.TryRemove(secondDriverKey, out _);
            PoolManager.Drivers.TryRemove(firstDriverKey, out _);
        }

        measurements.Clear();
        listener.RecordObservableInstruments();
        Assert.Empty(measurements);
        return;

        static (IReader<string> Reader, Task Started) CreateReader(
            string grpcConnectionString,
            string readerName,
            int partitionSessionCount)
        {
            var responses = Channel.CreateUnbounded<FromServer>();
            responses.Writer.TryWrite(InitResponse);
            for (var partitionSessionId = 1; partitionSessionId <= partitionSessionCount; partitionSessionId++)
            {
                responses.Writer.TryWrite(StartPartitionSessionRequest(
                    partitionSessionId: partitionSessionId));
            }

            FromServer current = null!;
            var started = 0;
            var allStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var mockStream = new Mock<ReaderStream>();
            mockStream.Setup(stream => stream.MoveNextAsync()).Returns(async () =>
            {
                if (!await responses.Reader.WaitToReadAsync().ConfigureAwait(false))
                {
                    return false;
                }

                current = await responses.Reader.ReadAsync().ConfigureAwait(false);
                return true;
            });
            mockStream.Setup(stream => stream.Current).Returns(() => current);
            mockStream.Setup(stream => stream.Write(It.IsAny<FromClient>()))
                .Callback<FromClient>(message =>
                {
                    if (message.StartPartitionSessionResponse != null
                        && Interlocked.Increment(ref started) == partitionSessionCount)
                    {
                        allStarted.TrySetResult();
                    }
                })
                .Returns(Task.CompletedTask);
            mockStream.Setup(stream => stream.AuthToken()).ReturnsAsync((string?)null);
            mockStream.Setup(stream => stream.RequestStreamComplete()).Returns(() =>
            {
                responses.Writer.TryComplete();
                return Task.CompletedTask;
            });
            var mockDriver = new Mock<IDriver>();
            mockDriver.Setup(driver => driver.RegisterOwner()).Returns(true);
            mockDriver.Setup(driver => driver.BidirectionalStreamCall(
                It.IsAny<Method<FromClient, FromServer>>(),
                It.IsAny<GrpcRequestSettings>())).ReturnsAsync(mockStream.Object);
            mockDriver.Setup(driver => driver.DisposeAsync())
                .Callback(() => mockDriver.Setup(driver => driver.IsDisposed).Returns(true));
            mockDriver.Setup(driver => driver.LoggerFactory).Returns(Utils.LoggerFactory);
            var reader = new ReaderBuilder<string>(new IDriverFactoryMock(
                mockDriver,
                grpcConnectionString,
                endpoint: "logical.ydb.test:2136",
                database: "/database"))
            {
                ConsumerName = "shared-consumer",
                ReaderName = readerName,
                SubscribeSettings = { new SubscribeSettings("/topic") }
            }.Build();

            return (reader, allStarted.Task);
        }
    }

    [Fact]
    public void PartitionSessionCount_GeneratesUniqueFallbackReaderNames()
    {
        var measurements = new List<Measurement>();
        using var listener = CreatePartitionSessionCountListener(measurements, "fallback-consumer");
        var firstMetrics = new ReaderMetricsReporter(
            "logical.ydb.test:2136", "/database", "fallback-consumer", readerName: null,
            readerStats: static () => new ReaderStats(1));
        var secondMetrics = new ReaderMetricsReporter(
            "logical.ydb.test:2136", "/database", "fallback-consumer", readerName: null,
            readerStats: static () => new ReaderStats(2));

        try
        {
            listener.RecordObservableInstruments();

            var firstReaderName = ReaderName(measurements[0]);
            var secondReaderName = ReaderName(measurements[1]);
            Assert.StartsWith("reader-", firstReaderName);
            Assert.StartsWith("reader-", secondReaderName);
            Assert.NotEqual(firstReaderName, secondReaderName);
            AssertReaderMeasurement(
                measurements[0], 1, firstReaderName, consumer: "fallback-consumer");
            AssertReaderMeasurement(
                measurements[1], 2, secondReaderName, consumer: "fallback-consumer");
        }
        finally
        {
            secondMetrics.Dispose();
            firstMetrics.Dispose();
        }
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void ConsumerAttribute_MatchesConfiguration(string? consumer)
    {
        var measurements = new List<Measurement>();
        var metricNames = MetricNames.Append(PartitionSessionCountMetricName).ToHashSet();
        var readerName = consumer is null ? "null-consumer-reader" : "empty-consumer-reader";
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument.Meter.Name == "Ydb.Sdk.Topic" && metricNames.Contains(instrument.Name))
            {
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            var metricTags = tags.ToArray();
            if (metricTags.Any(tag => tag.Key == "reader.name" && Equals(tag.Value, readerName)))
            {
                measurements.Add(new Measurement(instrument.Name, value, metricTags));
            }
        });
        listener.Start();

        using var metrics = new ReaderMetricsReporter(
            endpoint: "localhost:2136",
            database: "/local",
            consumer: consumer,
            readerName: readerName,
            readerStats: static () => new ReaderStats(1));

        metrics.ReportReceived(1, "/topic");
        metrics.ReportDelivered(1, "/topic");
        metrics.ReportCommitQueued(1, "/topic");
        metrics.ReportCommitAcknowledged(1, "/topic");
        listener.RecordObservableInstruments();

        Assert.Equal(5, measurements.Count);
        Assert.All(measurements, measurement =>
        {
            var expectedTags = new List<KeyValuePair<string, object?>>
            {
                new("endpoint", "localhost:2136"),
                new("database", "/local")
            };
            if (consumer is not null)
            {
                expectedTags.Add(new KeyValuePair<string, object?>("consumer", consumer));
            }

            expectedTags.Add(new KeyValuePair<string, object?>("reader.name", readerName));
            if (measurement.InstrumentName != PartitionSessionCountMetricName)
            {
                expectedTags.Add(new KeyValuePair<string, object?>("topic", "/topic"));
            }

            Assert.Equal(expectedTags, measurement.Tags);
        });
    }

    [Theory]
    [InlineData("configured-reader")]
    [InlineData(null)]
    public async Task PartitionSessionCount_ReaderNameStaysStableOnReconnect(string? configuredReaderName)
    {
        var consumer = $"reconnect-{configuredReaderName ?? "fallback"}-consumer";
        var grpcConnectionString = $"Reader_Metrics_Name_Reconnect_{configuredReaderName ?? "Fallback"}";
        var measurements = new List<Measurement>();
        using var listener = CreatePartitionSessionCountListener(measurements, consumer);
        var firstSessionClosed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var lastMoveNext = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstSessionStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondSessionStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var startedSessions = 0;
        var mockStream = new Mock<ReaderStream>();
        mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(firstSessionClosed.Task)
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(lastMoveNext.Task);
        mockStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponse)
            .Returns(StartPartitionSessionRequest())
            .Returns(InitResponse)
            .Returns(StartPartitionSessionRequest());
        mockStream.Setup(stream => stream.Write(It.IsAny<FromClient>()))
            .Callback<FromClient>(message =>
            {
                if (message.StartPartitionSessionResponse == null)
                {
                    return;
                }

                if (Interlocked.Increment(ref startedSessions) == 1)
                {
                    firstSessionStarted.TrySetResult();
                }
                else
                {
                    secondSessionStarted.TrySetResult();
                }
            })
            .Returns(Task.CompletedTask);
        mockStream.Setup(stream => stream.AuthToken()).ReturnsAsync((string?)null);
        mockStream.Setup(stream => stream.RequestStreamComplete()).Returns(() =>
        {
            firstSessionClosed.TrySetResult(false);
            lastMoveNext.TrySetResult(false);
            return Task.CompletedTask;
        });
        var mockDriver = new Mock<IDriver>();
        mockDriver.Setup(driver => driver.RegisterOwner()).Returns(true);
        mockDriver.Setup(driver => driver.BidirectionalStreamCall(
            It.IsAny<Method<FromClient, FromServer>>(),
            It.IsAny<GrpcRequestSettings>())).ReturnsAsync(mockStream.Object);
        mockDriver.Setup(driver => driver.DisposeAsync())
            .Callback(() => mockDriver.Setup(driver => driver.IsDisposed).Returns(true));
        mockDriver.Setup(driver => driver.LoggerFactory).Returns(Utils.LoggerFactory);
        var reader = new ReaderBuilder<string>(new IDriverFactoryMock(
            mockDriver,
            grpcConnectionString,
            endpoint: "logical.ydb.test:2136",
            database: "/database"))
        {
            ConsumerName = consumer,
            ReaderName = configuredReaderName,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        try
        {
            await firstSessionStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            var firstMeasurement = ObserveSingleMeasurement();
            var readerName = ReaderName(firstMeasurement);
            if (configuredReaderName == null)
            {
                Assert.StartsWith("reader-", readerName);
            }
            else
            {
                Assert.Equal(configuredReaderName, readerName);
            }

            AssertReaderMeasurement(firstMeasurement, 1, readerName, consumer: consumer);

            firstSessionClosed.SetResult(false);
            await secondSessionStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            AssertReaderMeasurement(ObserveSingleMeasurement(), 1, readerName, consumer: consumer);
        }
        finally
        {
            await reader.DisposeAsync();
            PoolManager.Drivers.TryRemove(grpcConnectionString, out _);
        }

        return;

        Measurement ObserveSingleMeasurement()
        {
            measurements.Clear();
            listener.RecordObservableInstruments();
            return Assert.Single(measurements);
        }
    }

    [Fact]
    public void ReportLifecycleEvents_RecordsFourCountersWithReaderAttributes()
    {
        var instruments = new List<Instrument>();
        var measurements = new List<Measurement>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, meterListener) =>
        {
            if (instrument.Meter.Name == "Ydb.Sdk.Topic" && MetricNames.Contains(instrument.Name))
            {
                instruments.Add(instrument);
                meterListener.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            var metricTags = tags.ToArray();
            if (metricTags.Any(tag => tag.Key == "consumer" && Equals(tag.Value, "orders-consumer")))
            {
                measurements.Add(new Measurement(instrument.Name, value, metricTags));
            }
        });
        listener.Start();

        using var metrics = new ReaderMetricsReporter(
            endpoint: "localhost:2136",
            database: "/local",
            consumer: "orders-consumer",
            readerName: "orders-reader",
            readerStats: static () => default);

        metrics.ReportReceived(2, "/orders");
        metrics.ReportDelivered(3, "/orders");
        metrics.ReportCommitQueued(4, "/orders");
        metrics.ReportCommitAcknowledged(5, "/orders");

        Assert.Equal(MetricNames, instruments.Select(instrument => instrument.Name));
        Assert.All(instruments, instrument => Assert.Equal("{message}", instrument.Unit));

        Assert.Collection(measurements,
            measurement => AssertMeasurement(measurement, MetricNames[0], 2),
            measurement => AssertMeasurement(measurement, MetricNames[1], 3),
            measurement => AssertMeasurement(measurement, MetricNames[2], 4),
            measurement => AssertMeasurement(measurement, MetricNames[3], 5));
    }

    [Fact]
    public async Task ReaderLifecycle_RecordsCountersForMessageAndBatch()
    {
        var measurements = new ConcurrentQueue<Measurement>();
        var acknowledgedMetrics = Channel.CreateUnbounded<long>();
        using var listener = CreateReaderMetricsListener(measurements, acknowledgedMetrics.Writer);
        var mockStream = new Mock<ReaderStream>();
        var mockDriver = new Mock<IDriver>();
        mockDriver.Setup(driver => driver.BidirectionalStreamCall(
            It.IsAny<Method<FromClient, FromServer>>(),
            It.IsAny<GrpcRequestSettings>())).ReturnsAsync(mockStream.Object);
        mockDriver.Setup(driver => driver.DisposeAsync())
            .Callback(() => mockDriver.Setup(driver => driver.IsDisposed).Returns(true));
        mockDriver.Setup(driver => driver.LoggerFactory).Returns(Utils.LoggerFactory);
        var driverFactory = new IDriverFactoryMock(mockDriver, "Reader_Metrics");
        var lastMoveNext = new TaskCompletionSource<bool>();
        var firstCommitReady = new TaskCompletionSource<bool>();
        var secondReadReady = new TaskCompletionSource<bool>();
        var batchCommitReady = new TaskCompletionSource<bool>();
        mockStream.Setup(stream => stream.RequestStreamComplete()).Returns(() =>
        {
            lastMoveNext.TrySetResult(false);
            firstCommitReady.TrySetResult(false);
            secondReadReady.TrySetResult(false);
            batchCommitReady.TrySetResult(false);
            return Task.CompletedTask;
        });

        mockStream.Setup(stream => stream.Write(It.IsAny<FromClient>())).Returns(Task.CompletedTask);

        mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(firstCommitReady.Task)
            .Returns(secondReadReady.Task)
            .Returns(batchCommitReady.Task)
            .Returns(lastMoveNext.Task);

        mockStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponse)
            .Returns(StartPartitionSessionRequest())
            .Returns(ReadResponse("First"u8.ToArray()))
            .Returns(CommitOffsetResponse())
            .Returns(ReadResponse(1, "Second"u8.ToArray(), "Third"u8.ToArray()))
            .Returns(CommitOffsetResponse(3));

        await using var reader = new ReaderBuilder<string>(driverFactory)
        {
            ConsumerName = "Metrics Consumer",
            ReaderName = "Metrics Reader",
            MemoryUsageMaxBytes = 1000,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        var timeout = TimeSpan.FromSeconds(5);
        var message = await reader.ReadAsync().AsTask().WaitAsync(timeout);
        var commitTask = message.CommitAsync();
        firstCommitReady.SetResult(true);
        await commitTask.WaitAsync(timeout);
        Assert.Equal(1, await acknowledgedMetrics.Reader.ReadAsync().AsTask().WaitAsync(timeout));
        AssertMetricValues(1);

        measurements.Clear();
        secondReadReady.SetResult(true);

        var batch = await reader.ReadBatchAsync().AsTask().WaitAsync(timeout);
        var batchCommitTask = batch.CommitBatchAsync();
        batchCommitReady.SetResult(true);
        await batchCommitTask.WaitAsync(timeout);
        Assert.Equal(2, await acknowledgedMetrics.Reader.ReadAsync().AsTask().WaitAsync(timeout));
        AssertMetricValues(2);
        return;

        void AssertMetricValues(long value)
        {
            Assert.All(MetricNames, name => Assert.Equal(value, MetricValue(name)));
            Assert.Equal(MetricNames.Length, measurements.Count);
            Assert.All(measurements, measurement =>
                AssertTags(measurement, "/topic", "Metrics Consumer", "Metrics Reader"));
        }

        long MetricValue(string name)
        {
            return measurements
                .Where(measurement => measurement.InstrumentName == name)
                .Sum(measurement => measurement.Value);
        }
    }

    private static void AssertMeasurement(Measurement measurement, string name, long value)
    {
        Assert.Equal(name, measurement.InstrumentName);
        Assert.Equal(value, measurement.Value);
        AssertTags(measurement, "/orders", "orders-consumer", "orders-reader");
    }

    private static MeterListener CreateReaderMetricsListener(
        ConcurrentQueue<Measurement> measurements,
        ChannelWriter<long> acknowledgedMetrics)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Meter.Name == "Ydb.Sdk.Topic" && MetricNames.Contains(instrument.Name))
                {
                    meterListener.EnableMeasurementEvents(instrument);
                }
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            var metricTags = tags.ToArray();
            if (metricTags.Any(tag => tag is { Key: "consumer", Value: "Metrics Consumer" }))
            {
                measurements.Enqueue(new Measurement(instrument.Name, value, metricTags));
                if (instrument.Name == MetricNames[3])
                {
                    acknowledgedMetrics.TryWrite(value);
                }
            }
        });
        listener.Start();
        return listener;
    }

    private static MeterListener CreatePartitionSessionCountListener(
        List<Measurement> measurements,
        string consumer,
        Action<Instrument>? instrumentPublished = null)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument is ObservableGauge<long>
                    && instrument.Meter.Name == "Ydb.Sdk.Topic"
                    && instrument.Name == PartitionSessionCountMetricName)
                {
                    instrumentPublished?.Invoke(instrument);
                    meterListener.EnableMeasurementEvents(instrument);
                }
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            var metricTags = tags.ToArray();
            if (instrument.Name == PartitionSessionCountMetricName
                && metricTags.Any(tag => tag.Key == "consumer" && Equals(tag.Value, consumer)))
            {
                measurements.Add(new Measurement(instrument.Name, value, metricTags));
            }
        });
        listener.Start();
        return listener;
    }

    private static void AssertTags(Measurement measurement, string topic, string consumer, string readerName) =>
        Assert.Collection(
            measurement.Tags,
            tag => AssertTag(tag, "endpoint", "localhost:2136"),
            tag => AssertTag(tag, "database", "/local"),
            tag => AssertTag(tag, "consumer", consumer),
            tag => AssertTag(tag, "reader.name", readerName),
            tag => AssertTag(tag, "topic", topic)
        );

    private static void AssertTag(KeyValuePair<string, object?> tag, string key, string value)
    {
        Assert.Equal(key, tag.Key);
        Assert.Equal(value, tag.Value);
    }

    private static void AssertReaderMeasurement(
        Measurement measurement,
        long value,
        string readerName,
        string consumer = "shared-consumer")
    {
        Assert.Equal(value, measurement.Value);
        Assert.Collection(measurement.Tags,
            tag => AssertTag(tag, "endpoint", "logical.ydb.test:2136"),
            tag => AssertTag(tag, "database", "/database"),
            tag => AssertTag(tag, "consumer", consumer),
            tag => AssertTag(tag, "reader.name", readerName));
    }

    private static string ReaderName(Measurement measurement) =>
        Assert.IsType<string>(Assert.Single(measurement.Tags, tag => tag.Key == "reader.name").Value);

    private static FromServer StopPartitionSessionRequest(long partitionSessionId = 1) => new()
    {
        Status = StatusIds.Types.StatusCode.Success,
        StopPartitionSessionRequest = new StreamReadMessage.Types.StopPartitionSessionRequest
        {
            PartitionSessionId = partitionSessionId,
            Graceful = true
        }
    };

    private sealed record Measurement(
        string InstrumentName,
        long Value,
        KeyValuePair<string, object?>[] Tags);
}
