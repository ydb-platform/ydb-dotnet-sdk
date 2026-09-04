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
        var mockStream = new Mock<ReaderStream>();
        var driverFactory = CreateDriverFactory(mockStream, "Reader_Partition_Session_Count_Metrics");
        var responses = Channel.CreateUnbounded<(bool HasNext, FromServer? Response)>();
        var handledEvents = Channel.CreateUnbounded<long>();
        SetupResponseStream(mockStream, responses, handledEvents.Writer);
        var reader = new ReaderBuilder<string>(driverFactory)
        {
            ConsumerName = "partition-count-consumer",
            ReaderName = "partition-count-reader",
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        try
        {
            await SendResponse(listener, InitResponse, 0, 0);
            await SendResponse(listener, StartPartitionSessionRequest(partitionSessionId: 1), 1, 1);
            await SendResponse(listener, StartPartitionSessionRequest(partitionSessionId: 2), 2, 2);
            await SendResponse(listener, StopPartitionSessionRequest(partitionSessionId: 1), -1, 1);
            await SendResponse(listener, StopPartitionSessionRequest(partitionSessionId: 2), -2, 0);
            await responses.Writer.WriteAsync((false, null));
            await SendResponse(listener, InitResponse, 0, 0);
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

        void AssertPartitionSessionCount(MeterListener meterListener, long expected)
        {
            measurements.Clear();
            meterListener.RecordObservableInstruments();
            var measurement = Assert.Single(measurements);
            Assert.Equal(expected, measurement.Value);
            KeyValuePair<string, object?>[] expectedTags =
            [
                new("endpoint", "localhost:2136"),
                new("database", "/local"),
                new("consumer", "partition-count-consumer"),
                new("reader.name", "partition-count-reader")
            ];
            Assert.Equal(expectedTags, measurement.Tags);
        }

        async Task SendResponse(
            MeterListener meterListener,
            FromServer response,
            long expectedEvent,
            long expectedCount)
        {
            await responses.Writer.WriteAsync((true, response));
            Assert.Equal(expectedEvent, await handledEvents.Reader.ReadAsync().AsTask().WaitAsync(timeout));
            AssertPartitionSessionCount(meterListener, expectedCount);
        }
    }

    [Fact]
    public void PartitionSessionCount_ReportsReadersSeparatelyWithFallbackNames()
    {
        const string endpoint = "two-readers.ydb.test:2136";
        var measurements = new List<Measurement>();
        using var listener = CreatePartitionSessionCountListener(measurements, endpoint);
        using var first = new ReaderMetricsReporter(
            endpoint, "/database", consumer: null, readerName: null,
            readerStats: static () => new ReaderStats(1));
        using var second = new ReaderMetricsReporter(
            endpoint, "/database", consumer: null, readerName: null,
            readerStats: static () => new ReaderStats(2));

        listener.RecordObservableInstruments();

        var firstReaderName = ReaderName(measurements[0]);
        var secondReaderName = ReaderName(measurements[1]);
        Assert.StartsWith("reader-", firstReaderName);
        Assert.StartsWith("reader-", secondReaderName);
        Assert.NotEqual(firstReaderName, secondReaderName);
        KeyValuePair<string, object?>[] firstTags =
            [new("endpoint", endpoint), new("database", "/database"), new("reader.name", firstReaderName)];
        KeyValuePair<string, object?>[] secondTags =
            [new("endpoint", endpoint), new("database", "/database"), new("reader.name", secondReaderName)];
        Assert.Equal(2, measurements.Count);
        Assert.Equal(1, measurements[0].Value);
        Assert.Equal(firstTags, measurements[0].Tags);
        Assert.Equal(2, measurements[1].Value);
        Assert.Equal(secondTags, measurements[1].Tags);
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

    private static string ReaderName(Measurement measurement) =>
        Assert.IsType<string>(Assert.Single(measurement.Tags, tag => tag.Key == "reader.name").Value);

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
}
