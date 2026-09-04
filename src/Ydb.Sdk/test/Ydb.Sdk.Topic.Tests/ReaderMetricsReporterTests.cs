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
    private static readonly string[] MetricNames =
    [
        "ydb.topic.reader.received.messages",
        "ydb.topic.reader.delivered.messages",
        "ydb.topic.reader.commit.queued",
        "ydb.topic.reader.commit.acknowledged"
    ];

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
