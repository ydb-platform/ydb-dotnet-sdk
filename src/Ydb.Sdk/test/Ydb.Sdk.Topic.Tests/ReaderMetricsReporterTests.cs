using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Threading.Channels;
using Grpc.Core;
using Moq;
using Xunit;
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

    private readonly IDriverFactoryMock _driverFactoryMock;
    private readonly Mock<ReaderStream> _mockStream = new();
    private readonly Task<bool> _lastMoveNext;

    public ReaderMetricsReporterTests()
    {
        var mockDriver = new Mock<IDriver>();
        mockDriver.Setup(driver => driver.BidirectionalStreamCall(
            It.IsAny<Method<FromClient, FromServer>>(),
            It.IsAny<GrpcRequestSettings>())).ReturnsAsync(_mockStream.Object);
        mockDriver.Setup(driver => driver.DisposeAsync())
            .Callback(() => mockDriver.Setup(driver => driver.IsDisposed).Returns(true));
        mockDriver.Setup(driver => driver.LoggerFactory).Returns(Utils.LoggerFactory);

        _driverFactoryMock = new IDriverFactoryMock(mockDriver, "Reader_Partition_Session_Count_Metrics");

        var lastMoveNext = new TaskCompletionSource<bool>();
        _lastMoveNext = lastMoveNext.Task;
        _mockStream.Setup(stream => stream.RequestStreamComplete()).Returns(() =>
        {
            lastMoveNext.TrySetResult(false);
            return Task.CompletedTask;
        });
    }

    [Fact]
    public async Task PartitionSessionCount_TracksSessionsAcrossReconnectAndDispose()
    {
        var timeout = TimeSpan.FromSeconds(5);
        var measurements = new List<Measurement>();
        Instrument? publishedInstrument = null;
        using var listener = CreatePartitionSessionCountListener(
            measurements,
            "localhost:2136",
            "partition-count-consumer",
            instrument => publishedInstrument = instrument);
        var startFirst = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var startSecond = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var stopFirst = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var stopSecond = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var reconnect = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var initialized = Channel.CreateUnbounded<bool>();
        var started = Channel.CreateUnbounded<long>();
        var stopped = Channel.CreateUnbounded<long>();
        _mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .Returns(startFirst.Task)
            .Returns(startSecond.Task)
            .Returns(stopFirst.Task)
            .Returns(stopSecond.Task)
            .Returns(reconnect.Task)
            .ReturnsAsync(true)
            .Returns(_lastMoveNext);
        _mockStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponse)
            .Returns(StartPartitionSessionRequest(partitionSessionId: 1))
            .Returns(StartPartitionSessionRequest(partitionSessionId: 2))
            .Returns(StopPartitionSessionRequest(partitionSessionId: 1))
            .Returns(StopPartitionSessionRequest(partitionSessionId: 2))
            .Returns(InitResponse);
        _mockStream.Setup(stream => stream.Write(It.IsAny<FromClient>()))
            .Callback<FromClient>(message =>
            {
                if (message.ReadRequest != null)
                {
                    initialized.Writer.TryWrite(true);
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
        var reader = new ReaderBuilder<string>(_driverFactoryMock)
        {
            ConsumerName = "partition-count-consumer",
            ReaderName = "partition-count-reader",
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        try
        {
            await initialized.Reader.ReadAsync().AsTask().WaitAsync(timeout);
            AssertPartitionSessionCount(0);

            startFirst.SetResult(true);
            Assert.Equal(1, await started.Reader.ReadAsync().AsTask().WaitAsync(timeout));
            AssertPartitionSessionCount(1);

            startSecond.SetResult(true);
            Assert.Equal(2, await started.Reader.ReadAsync().AsTask().WaitAsync(timeout));
            AssertPartitionSessionCount(2);

            stopFirst.SetResult(true);
            Assert.Equal(1, await stopped.Reader.ReadAsync().AsTask().WaitAsync(timeout));
            AssertPartitionSessionCount(1);

            stopSecond.SetResult(true);
            Assert.Equal(2, await stopped.Reader.ReadAsync().AsTask().WaitAsync(timeout));
            AssertPartitionSessionCount(0);

            reconnect.SetResult(false);
            await initialized.Reader.ReadAsync().AsTask().WaitAsync(timeout);
            AssertPartitionSessionCount(0);
        }
        finally
        {
            await reader.DisposeAsync();
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
                tag => AssertTag(tag, "endpoint", "localhost:2136"),
                tag => AssertTag(tag, "database", "/local"),
                tag => AssertTag(tag, "consumer", "partition-count-consumer"),
                tag => AssertTag(tag, "reader.name", "partition-count-reader"));
        }
    }

    [Fact]
    public void PartitionSessionCount_ReportsReadersSeparatelyWithFallbackNames()
    {
        const string endpoint = "two-readers.ydb.test:2136";
        var measurements = new List<Measurement>();
        using var listener = CreatePartitionSessionCountListener(measurements, endpoint);
        var first = new ReaderMetricsReporter(
            endpoint, "/database", consumer: null, readerName: null,
            readerStats: static () => new ReaderStats(1));
        var second = new ReaderMetricsReporter(
            endpoint, "/database", consumer: null, readerName: null,
            readerStats: static () => new ReaderStats(2));

        try
        {
            listener.RecordObservableInstruments();

            var firstReaderName = ReaderName(measurements[0]);
            var secondReaderName = ReaderName(measurements[1]);
            Assert.StartsWith("reader-", firstReaderName);
            Assert.StartsWith("reader-", secondReaderName);
            Assert.NotEqual(firstReaderName, secondReaderName);
            Assert.Collection(measurements,
                measurement => AssertReaderMeasurement(measurement, 1, endpoint, firstReaderName),
                measurement => AssertReaderMeasurement(measurement, 2, endpoint, secondReaderName));
        }
        finally
        {
            second.Dispose();
            first.Dispose();
        }

        measurements.Clear();
        listener.RecordObservableInstruments();
        Assert.Empty(measurements);
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
            if (metricTags.Any(tag => tag.Key == "reader.name" && Equals(tag.Value, "orders-reader")))
            {
                measurements.Add(new Measurement(instrument.Name, value, metricTags));
            }
        });
        listener.Start();

        using var metrics = new ReaderMetricsReporter(
            endpoint: "localhost:2136",
            database: "/local",
            consumer: null,
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
        AssertTags(measurement, "/orders", consumer: null, readerName: "orders-reader");
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
        string endpoint,
        string? consumer = null,
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
                && metricTags.Any(tag => tag.Key == "endpoint" && Equals(tag.Value, endpoint))
                && (consumer is null
                    || metricTags.Any(tag => tag.Key == "consumer" && Equals(tag.Value, consumer))))
            {
                measurements.Add(new Measurement(instrument.Name, value, metricTags));
            }
        });
        listener.Start();
        return listener;
    }

    private static void AssertTag(KeyValuePair<string, object?> tag, string key, string value)
    {
        Assert.Equal(key, tag.Key);
        Assert.Equal(value, tag.Value);
    }

    private static void AssertReaderMeasurement(
        Measurement measurement,
        long value,
        string endpoint,
        string readerName)
    {
        Assert.Equal(value, measurement.Value);
        Assert.Collection(measurement.Tags,
            tag => AssertTag(tag, "endpoint", endpoint),
            tag => AssertTag(tag, "database", "/database"),
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

    private static void AssertTags(Measurement measurement, string topic, string? consumer, string readerName)
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
        expectedTags.Add(new KeyValuePair<string, object?>("topic", topic));
        Assert.Equal(expectedTags, measurement.Tags);
    }

    private sealed record Measurement(
        string InstrumentName,
        long Value,
        KeyValuePair<string, object?>[] Tags);
}
