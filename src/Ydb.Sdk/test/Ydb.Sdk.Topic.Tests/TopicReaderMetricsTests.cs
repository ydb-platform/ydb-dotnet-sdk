using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Moq;
using Xunit;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Topic.Reader;
using Ydb.Topic;

namespace Ydb.Sdk.Topic.Tests;

using ReaderStream = IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>;
using FromClient = StreamReadMessage.Types.FromClient;
using FromServer = StreamReadMessage.Types.FromServer;

public class TopicReaderMetricsTests
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
        using var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Meter.Name == "Ydb.Sdk" && MetricNames.Contains(instrument.Name))
                {
                    instruments.Add(instrument);
                    meterListener.EnableMeasurementEvents(instrument);
                }
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            var metricTags = tags.ToArray();
            if (metricTags.Contains(new KeyValuePair<string, object?>("consumer", "orders-consumer")))
            {
                measurements.Add(new Measurement(instrument.Name, value, metricTags));
            }
        });
        listener.Start();

        var metrics = new TopicReaderMetrics(
            endpoint: "localhost:2136",
            database: "/local",
            consumer: "orders-consumer");

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
    public async Task ReaderLifecycle_RecordsExpectedCountersForRepeatedCommitAndAcknowledgement()
    {
        var measurements = new ConcurrentQueue<Measurement>();
        using var listener = CreateReaderMetricsListener(measurements);
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
        mockStream.Setup(stream => stream.RequestStreamComplete()).Returns(() =>
        {
            lastMoveNext.TrySetResult(false);
            return Task.CompletedTask;
        });
        var readResponseReady = new TaskCompletionSource<bool>();
        var commitsReady = new TaskCompletionSource<bool>();
        var duplicateAckHandled = new TaskCompletionSource();

        mockStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                readResponseReady.SetResult(true);
                return Task.CompletedTask;
            })
            .Returns(Task.CompletedTask)
            .Returns(() =>
            {
                commitsReady.SetResult(true);
                return Task.CompletedTask;
            });

        mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(readResponseReady.Task)
            .Returns(commitsReady.Task)
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(() =>
            {
                duplicateAckHandled.SetResult();
                return lastMoveNext.Task;
            });

        mockStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponse())
            .Returns(StartPartitionSessionRequest())
            .Returns(ReadResponse("First"u8.ToArray(), "Second"u8.ToArray()))
            .Returns(CommitOffsetResponse(2))
            .Returns(CommitOffsetResponse(2))
            .Returns(CommitOffsetResponse(1));

        await using var reader = new ReaderBuilder<string>(driverFactory)
        {
            ConsumerName = "Metrics Consumer",
            MemoryUsageMaxBytes = 1000,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        var batch = await reader.ReadBatchAsync();
        await Task.WhenAll(batch.CommitBatchAsync(), batch.CommitBatchAsync());
        await duplicateAckHandled.Task.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(2, MetricValue(MetricNames[0]));
        Assert.Equal(2, MetricValue(MetricNames[1]));
        Assert.Equal(4, MetricValue(MetricNames[2]));
        Assert.Equal(4, MetricValue(MetricNames[3]));
        Assert.All(measurements, measurement => AssertTags(measurement, "/topic", "Metrics Consumer"));

        long MetricValue(string name) => measurements
            .Where(measurement => measurement.InstrumentName == name)
            .Sum(measurement => measurement.Value);
    }

    private static void AssertMeasurement(Measurement measurement, string name, long value)
    {
        Assert.Equal(name, measurement.InstrumentName);
        Assert.Equal(value, measurement.Value);
        AssertTags(measurement, "/orders", "orders-consumer");
    }

    private static MeterListener CreateReaderMetricsListener(ConcurrentQueue<Measurement> measurements)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, meterListener) =>
            {
                if (instrument.Meter.Name == "Ydb.Sdk" && MetricNames.Contains(instrument.Name))
                {
                    meterListener.EnableMeasurementEvents(instrument);
                }
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            var metricTags = tags.ToArray();
            if (metricTags.Contains(new KeyValuePair<string, object?>("consumer", "Metrics Consumer")))
            {
                measurements.Enqueue(new Measurement(instrument.Name, value, metricTags));
            }
        });
        listener.Start();
        return listener;
    }

    private static void AssertTags(Measurement measurement, string topic, string consumer)
    {
        Assert.Collection(measurement.Tags,
            tag => Assert.Equal(new KeyValuePair<string, object?>("endpoint", "localhost:2136"), tag),
            tag => Assert.Equal(new KeyValuePair<string, object?>("database", "/local"), tag),
            tag => Assert.Equal(new KeyValuePair<string, object?>("topic", topic), tag),
            tag => Assert.Equal(new KeyValuePair<string, object?>("consumer", consumer), tag));
    }

    private static FromServer InitResponse() => new()
    {
        Status = StatusIds.Types.StatusCode.Success,
        InitResponse = new StreamReadMessage.Types.InitResponse { SessionId = "SessionId" }
    };

    private static FromServer StartPartitionSessionRequest() => new()
    {
        Status = StatusIds.Types.StatusCode.Success,
        StartPartitionSessionRequest = new StreamReadMessage.Types.StartPartitionSessionRequest
        {
            PartitionOffsets = new OffsetsRange { End = 1000 },
            PartitionSession = new StreamReadMessage.Types.PartitionSession
            { Path = "/topic", PartitionId = 1, PartitionSessionId = 1 }
        }
    };

    private static FromServer ReadResponse(params byte[][] messages)
    {
        var batch = new StreamReadMessage.Types.ReadResponse.Types.Batch { ProducerId = "ProducerId" };
        for (var offset = 0; offset < messages.Length; offset++)
        {
            batch.MessageData.Add(new StreamReadMessage.Types.ReadResponse.Types.MessageData
            {
                Data = ByteString.CopyFrom(messages[offset]),
                Offset = offset,
                CreatedAt = new Timestamp()
            });
        }

        return new FromServer
        {
            Status = StatusIds.Types.StatusCode.Success,
            ReadResponse = new StreamReadMessage.Types.ReadResponse
            {
                BytesSize = 50,
                PartitionData =
                {
                    new StreamReadMessage.Types.ReadResponse.Types.PartitionData
                        { PartitionSessionId = 1, Batches = { batch } }
                }
            }
        };
    }

    private static FromServer CommitOffsetResponse(int committedOffset) => new()
    {
        Status = StatusIds.Types.StatusCode.Success,
        CommitOffsetResponse = new StreamReadMessage.Types.CommitOffsetResponse
        {
            PartitionsCommittedOffsets =
            {
                new StreamReadMessage.Types.CommitOffsetResponse.Types.PartitionCommittedOffset
                    { PartitionSessionId = 1, CommittedOffset = committedOffset }
            }
        }
    };

    private sealed record Measurement(
        string InstrumentName,
        long Value,
        KeyValuePair<string, object?>[] Tags);
}
