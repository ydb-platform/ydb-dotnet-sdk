using System.Threading.Channels;
using Moq;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using Xunit;
using Ydb.Sdk.OpenTelemetry;
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
    private const string PartitionSessionCountReaderName = "partition-count-reader";
    private const string LifecycleReaderName = "reader-lifecycle-metrics";

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
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);
        var mockStream = new Mock<ReaderStream>();
        var driverFactory = CreateDriverFactory(mockStream, "Reader_Partition_Session_Count_Metrics");
        var responses = Channel.CreateUnbounded<(bool HasNext, FromServer? Response)>();
        var handledEvents = Channel.CreateUnbounded<long>();
        SetupResponseStream(mockStream, responses, handledEvents.Writer);
        var reader = new ReaderBuilder<string>(driverFactory)
        {
            ConsumerName = "partition-count-consumer",
            ReaderName = PartitionSessionCountReaderName,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        try
        {
            await SendResponse(meterProvider, InitResponse, 0, 0);
            Assert.Equal("{session}", GetMetric(exportedItems, PartitionSessionCountMetricName).Unit);
            await SendResponse(meterProvider, StartPartitionSessionRequest(partitionSessionId: 1), 1, 1);
            await SendResponse(meterProvider, StartPartitionSessionRequest(partitionSessionId: 2), 2, 2);
            await SendResponse(meterProvider, StopPartitionSessionRequest(partitionSessionId: 1), -1, 1);
            await SendResponse(meterProvider, StopPartitionSessionRequest(partitionSessionId: 2), -2, 0);
            await responses.Writer.WriteAsync((false, null));
            await SendResponse(meterProvider, InitResponse, 0, 0);
        }
        finally
        {
            await reader.DisposeAsync();
        }

        var afterDisposeItems = new List<Metric>();
        using var afterDisposeMeterProvider = CreateMeterProvider(afterDisposeItems);
        afterDisposeMeterProvider.ForceFlush();
        Assert.Empty(GetReaderPoints(afterDisposeItems, PartitionSessionCountMetricName,
            PartitionSessionCountReaderName));
        return;

        async Task SendResponse(
            MeterProvider provider,
            FromServer response,
            long expectedEvent,
            long expectedCount)
        {
            await responses.Writer.WriteAsync((true, response));
            Assert.Equal(expectedEvent, await handledEvents.Reader.ReadAsync().AsTask().WaitAsync(timeout));
            exportedItems.Clear();
            provider.ForceFlush();
            var point = Assert.Single(GetReaderPoints(exportedItems, PartitionSessionCountMetricName,
                PartitionSessionCountReaderName));
            Assert.Equal(expectedCount, point.GetGaugeLastValueLong());
            AssertTags(point, "partition-count-consumer", PartitionSessionCountReaderName);
        }
    }

    [Fact]
    public async Task ReaderLifecycle_RecordsCounters()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);
        var mockStream = new Mock<ReaderStream>();
        var driverFactory = CreateDriverFactory(mockStream, "Reader_Metrics");
        var lastMoveNext = new TaskCompletionSource<bool>();
        var firstCommitReady = new TaskCompletionSource<bool>();
        var firstCommitHandled = new TaskCompletionSource<bool>();
        var secondReadReady = new TaskCompletionSource<bool>();
        var batchCommitReady = new TaskCompletionSource<bool>();
        var batchCommitHandled = new TaskCompletionSource<bool>();
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
            .Returns(() =>
            {
                firstCommitHandled.SetResult(true);
                return secondReadReady.Task;
            })
            .Returns(batchCommitReady.Task)
            .Returns(() =>
            {
                batchCommitHandled.SetResult(true);
                return lastMoveNext.Task;
            });

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
            ReaderName = LifecycleReaderName,
            MemoryUsageMaxBytes = 1000,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        var timeout = TimeSpan.FromSeconds(5);
        var message = await reader.ReadAsync().AsTask().WaitAsync(timeout);
        var commitTask = message.CommitAsync();
        firstCommitReady.SetResult(true);
        await commitTask.WaitAsync(timeout);
        await firstCommitHandled.Task.WaitAsync(timeout);
        AssertMetricValues(1);

        secondReadReady.SetResult(true);

        var batch = await reader.ReadBatchAsync().AsTask().WaitAsync(timeout);
        var batchCommitTask = batch.CommitBatchAsync();
        batchCommitReady.SetResult(true);
        await batchCommitTask.WaitAsync(timeout);
        await batchCommitHandled.Task.WaitAsync(timeout);
        AssertMetricValues(3);
        return;

        void AssertMetricValues(long value)
        {
            exportedItems.Clear();
            meterProvider.ForceFlush();
            foreach (var name in MetricNames)
            {
                var point = Assert.Single(GetReaderPoints(exportedItems, name, LifecycleReaderName));
                Assert.Equal(value, point.GetSumLong());
                AssertTags(point, "Metrics Consumer", LifecycleReaderName, "/topic");
            }
        }
    }

    private static MeterProvider CreateMeterProvider(List<Metric> exportedItems) =>
        global::OpenTelemetry.Sdk.CreateMeterProviderBuilder()
            .AddYdbTopic()
            .AddInMemoryExporter(exportedItems)
            .Build();

    private static void SetupResponseStream(
        Mock<ReaderStream> stream,
        Channel<(bool HasNext, FromServer? Response)> responses,
        ChannelWriter<long> handledEvents)
    {
        FromServer?[] currentResponse = [null];
        stream.Setup(mock => mock.MoveNextAsync()).Returns(async () =>
        {
            var response = await responses.Reader.ReadAsync();
            currentResponse[0] = response.Response;
            return response.HasNext;
        });
        stream.Setup(mock => mock.Current).Returns(() => currentResponse[0]!);
        stream.Setup(mock => mock.RequestStreamComplete()).Returns(() =>
        {
            responses.Writer.TryWrite((false, null));
            return Task.CompletedTask;
        });
        stream.Setup(mock => mock.Write(It.IsAny<FromClient>()))
            .Callback<FromClient>(message =>
            {
                if (message.ReadRequest != null)
                {
                    handledEvents.TryWrite(0);
                }
                else if (message.StartPartitionSessionResponse != null)
                {
                    handledEvents.TryWrite(message.StartPartitionSessionResponse.PartitionSessionId);
                }
                else if (message.StopPartitionSessionResponse != null)
                {
                    handledEvents.TryWrite(-message.StopPartitionSessionResponse.PartitionSessionId);
                }
            })
            .Returns(Task.CompletedTask);
    }

    private static Metric GetMetric(List<Metric> exportedItems, string name) =>
        exportedItems.Single(metric => metric.Name == name);

    private static IEnumerable<MetricPoint> GetReaderPoints(
        List<Metric> exportedItems,
        string metricName,
        string readerName)
    {
        foreach (var point in exportedItems
                     .Where(metric => metric.Name == metricName)
                     .SelectMany(EnumeratePoints))
        {
            if (ToDictionary(point.Tags).GetValueOrDefault("reader.name") as string == readerName)
            {
                yield return point;
            }
        }
    }

    private static IEnumerable<MetricPoint> EnumeratePoints(Metric metric)
    {
        foreach (var point in metric.GetMetricPoints())
        {
            yield return point;
        }
    }

    private static void AssertTags(
        MetricPoint point,
        string consumer,
        string readerName,
        string? topic = null)
    {
        var tags = ToDictionary(point.Tags);
        Assert.Equal(topic is null ? 4 : 5, tags.Count);
        Assert.Equal("localhost:2136", tags["endpoint"]);
        Assert.Equal("/local", tags["database"]);
        Assert.Equal(consumer, tags["consumer"]);
        Assert.Equal(readerName, tags["reader.name"]);
        if (topic is not null)
        {
            Assert.Equal(topic, tags["topic"]);
        }
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
