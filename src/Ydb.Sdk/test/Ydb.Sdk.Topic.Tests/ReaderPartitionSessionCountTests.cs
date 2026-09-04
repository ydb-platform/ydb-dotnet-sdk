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

public class ReaderPartitionSessionCountTests
{
    private const string PartitionSessionCountMetricName = "ydb.topic.reader.partition_session.count";

    private readonly IDriverFactoryMock _driverFactoryMock;
    private readonly Mock<ReaderStream> _mockStream = new();
    private readonly Task<bool> _lastMoveNext;

    public ReaderPartitionSessionCountTests()
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
                measurements.Add(new Measurement(value, metricTags));
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

    private sealed record Measurement(long Value, KeyValuePair<string, object?>[] Tags);
}
