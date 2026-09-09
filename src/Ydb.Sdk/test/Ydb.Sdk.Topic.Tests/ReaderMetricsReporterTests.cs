using System.Text;
using System.Threading.Channels;
using Grpc.Core;
using Moq;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using Xunit;
using Ydb.Sdk.Ado;
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
    private const string CommitOffsetLagMetricName = "ydb.topic.reader.commit_offset.lag.max";
    private const string LifecycleReaderName = "reader-lifecycle-metrics";

    private static readonly string[] MetricNames =
    [
        "ydb.topic.reader.received.messages",
        "ydb.topic.reader.delivered.messages",
        "ydb.topic.reader.commit.queued",
        "ydb.topic.reader.commit.acknowledged"
    ];

    [Fact]
    public async Task CreditBalance_TracksOutstandingStreamCredit()
    {
        const string readerName = "credit-balance-reader";
        const string metricName = "ydb.topic.reader.credit_balance_bytes";
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);
        var mockStream = new Mock<ReaderStream>();
        var closed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var reconnectBlocked = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var reconnected = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var initializationCount = 0;
        mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(closed.Task)
            .Returns(reconnectBlocked.Task);
        mockStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponse)
            .Returns(StartPartitionSessionRequest())
            .Returns(ReadResponse("message"u8.ToArray()));
        mockStream.Setup(stream => stream.Write(It.IsAny<FromClient>()))
            .Callback<FromClient>(message =>
            {
                if (message.InitRequest is not null && Interlocked.Increment(ref initializationCount) == 2)
                {
                    reconnected.TrySetResult();
                }
            })
            .Returns(Task.CompletedTask);
        mockStream.Setup(stream => stream.RequestStreamComplete()).Returns(() =>
        {
            closed.TrySetResult(false);
            reconnectBlocked.TrySetResult(false);
            return Task.CompletedTask;
        });
        var reader = new ReaderBuilder<string>(CreateDriverFactory(mockStream, readerName))
        {
            ReaderName = readerName,
            ConsumerName = "credit-balance-consumer",
            MemoryUsageMaxBytes = 1000,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        await reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        meterProvider.ForceFlush();
        var metric = GetMetric(exportedItems, metricName);
        Assert.Equal(MetricType.LongGauge, metric.MetricType);
        Assert.Equal("By", metric.Unit);
        var point = Assert.Single(GetReaderPoints(exportedItems, metricName, readerName));
        Assert.Equal(950, point.GetGaugeLastValueLong());
        AssertTags(point, "credit-balance-consumer", readerName);

        closed.TrySetResult(false);
        await reconnected.Task.WaitAsync(TimeSpan.FromSeconds(5));
        exportedItems.Clear();
        meterProvider.ForceFlush();
        point = Assert.Single(GetReaderPoints(exportedItems, metricName, readerName));
        Assert.Equal(0, point.GetGaugeLastValueLong());

        await reader.DisposeAsync();
        reconnectBlocked.TrySetResult(false);
        var afterDisposeItems = new List<Metric>();
        using var afterDisposeMeterProvider = CreateMeterProvider(afterDisposeItems);
        afterDisposeMeterProvider.ForceFlush();
        Assert.Empty(GetReaderPoints(afterDisposeItems, metricName, readerName));
    }

    [Fact]
    public async Task ReceivedBytes_RecordsResponseSize()
    {
        const string readerName = "received-bytes-reader";
        const string metricName = "ydb.topic.reader.received.bytes";
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);
        var mockStream = new Mock<ReaderStream>();
        var closed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(closed.Task);
        mockStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponse)
            .Returns(StartPartitionSessionRequest())
            .Returns(ReadResponse("message"u8.ToArray()));
        mockStream.Setup(stream => stream.Write(It.IsAny<FromClient>())).Returns(Task.CompletedTask);
        mockStream.Setup(stream => stream.RequestStreamComplete()).Returns(() =>
        {
            closed.TrySetResult(false);
            return Task.CompletedTask;
        });
        await using var reader = new ReaderBuilder<string>(CreateDriverFactory(mockStream, readerName))
        {
            ReaderName = readerName,
            ConsumerName = "received-bytes-consumer",
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        await reader.ReadAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(5));
        meterProvider.ForceFlush();
        var metric = GetMetric(exportedItems, metricName);
        Assert.Equal(MetricType.LongSum, metric.MetricType);
        Assert.Equal("By", metric.Unit);
        var point = Assert.Single(GetReaderPoints(exportedItems, metricName, readerName));
        Assert.Equal(50, point.GetSumLong());
        AssertTags(point, "received-bytes-consumer", readerName);
    }

    [Fact]
    public async Task LocalBufferMessages_TracksDelivery()
    {
        const string readerName = "local-buffer-reader";
        const string metricName = "ydb.topic.reader.local_buffer.messages";
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);
        var mockStream = new Mock<ReaderStream>();
        var responseHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var closed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        mockStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(() =>
            {
                responseHandled.TrySetResult();
                return closed.Task;
            });
        mockStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponse)
            .Returns(StartPartitionSessionRequest())
            .Returns(ReadResponse("message"u8.ToArray()));
        mockStream.Setup(stream => stream.Write(It.IsAny<FromClient>())).Returns(Task.CompletedTask);
        mockStream.Setup(stream => stream.RequestStreamComplete()).Returns(() =>
        {
            closed.TrySetResult(false);
            return Task.CompletedTask;
        });
        await using var reader = new ReaderBuilder<string>(CreateDriverFactory(mockStream, readerName))
        {
            ReaderName = readerName,
            ConsumerName = "local-buffer-consumer",
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        await responseHandled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        AssertBufferedMessages(1);

        await reader.ReadAsync();
        AssertBufferedMessages(0);
        return;

        void AssertBufferedMessages(long expected)
        {
            exportedItems.Clear();
            meterProvider.ForceFlush();
            var metric = GetMetric(exportedItems, metricName);
            Assert.Equal(MetricType.LongSumNonMonotonic, metric.MetricType);
            Assert.Equal("{message}", metric.Unit);
            var point = Assert.Single(GetReaderPoints(exportedItems, metricName, readerName));
            Assert.Equal(expected, point.GetSumLong());
            AssertTags(point, "local-buffer-consumer", readerName, "/topic");
        }
    }

    [Fact]
    public async Task SessionErrors_RecordsOneRetryAndTerminalStop()
    {
        const string readerName = "session-errors-reader";
        const string metricName = "ydb.topic.reader.session.errors";
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);
        var firstStream = new Mock<ReaderStream>();
        var secondStream = new Mock<ReaderStream>();
        var fail = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var responseWaiting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var requestWaiting = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var transportError = new YdbException(StatusCode.ClientTransportUnavailable, "transport failure");

        firstStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(true)
            .Returns(async () =>
            {
                responseWaiting.SetResult();
                await fail.Task;
                throw transportError;
            });
        firstStream.SetupSequence(stream => stream.Current)
            .Returns(InitResponse)
            .Returns(StartPartitionSessionRequest());
        firstStream.SetupSequence(stream => stream.Write(It.IsAny<FromClient>()))
            .Returns(Task.CompletedTask)
            .Returns(Task.CompletedTask)
            .Returns(async () =>
            {
                requestWaiting.SetResult();
                await fail.Task;
                throw transportError;
            });
        firstStream.Setup(stream => stream.RequestStreamComplete()).Returns(Task.CompletedTask);

        secondStream.Setup(stream => stream.Write(It.IsAny<FromClient>())).Returns(Task.CompletedTask);
        secondStream.Setup(stream => stream.MoveNextAsync()).ReturnsAsync(true);
        secondStream.Setup(stream => stream.Current).Returns(new FromServer
        {
            Status = StatusIds.Types.StatusCode.Unauthorized
        });

        var driver = new Mock<IDriver>();
        driver.SetupSequence(mock => mock.BidirectionalStreamCall(
                It.IsAny<Method<FromClient, FromServer>>(),
                It.IsAny<GrpcRequestSettings>()))
            .ReturnsAsync(firstStream.Object)
            .ReturnsAsync(secondStream.Object);
        driver.Setup(mock => mock.LoggerFactory).Returns(Utils.LoggerFactory);
        await using var reader = new ReaderBuilder<string>(new IDriverFactoryMock(driver, "session-errors"))
        {
            ReaderName = readerName,
            ConsumerName = "session-errors-consumer",
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        var timeout = TimeSpan.FromSeconds(5);
        await Task.WhenAll(responseWaiting.Task, requestWaiting.Task).WaitAsync(timeout);
        fail.SetResult();
        await Assert.ThrowsAsync<ReaderException>(async () =>
            await reader.ReadAsync().AsTask().WaitAsync(timeout));

        meterProvider.ForceFlush();
        var metric = GetMetric(exportedItems, metricName);
        Assert.Equal(MetricType.LongSum, metric.MetricType);
        Assert.Equal("{error}", metric.Unit);
        var points = GetReaderPoints(exportedItems, metricName, readerName)
            .ToDictionary(point => ToDictionary(point.Tags)["retry_decision"]!.ToString()!);
        Assert.Equal(1, points["retry"].GetSumLong());
        Assert.Equal(1, points["stop"].GetSumLong());
        AssertSessionErrorTags(points["retry"], "retry", "ClientTransportUnavailable", "transport_error");
        AssertSessionErrorTags(points["stop"], "stop", "Unauthorized", "ydb_error");
        return;

        void AssertSessionErrorTags(
            MetricPoint point,
            string retryDecision,
            string statusCode,
            string errorType)
        {
            var tags = ToDictionary(point.Tags);
            Assert.Equal(7, tags.Count);
            Assert.Equal("localhost:2136", tags["endpoint"]);
            Assert.Equal("/local", tags["database"]);
            Assert.Equal("session-errors-consumer", tags["consumer"]);
            Assert.Equal(readerName, tags["reader.name"]);
            Assert.Equal(retryDecision, tags["retry_decision"]);
            Assert.Equal(statusCode, tags["status_code"]);
            Assert.Equal(errorType, tags["error.type"]);
            Assert.DoesNotContain("topic", tags);
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LocalBufferMessageAgeMax_TracksFifoAcrossTopicsUntilBatchRemoval(bool readLastAsBatch)
    {
        const string readerName = "message-age-reader";
        const string metricName = "ydb.topic.reader.local_buffer.message_age.max";
        var timeout = TimeSpan.FromSeconds(5);
        var exportedItems = new List<Metric>();
        Assert.Equal(0, ReaderMetricsReporter.ReportReadResponseStart());
        using var meterProvider = CreateMeterProvider(exportedItems);
        Assert.True(ReaderMetricsReporter.ReportReadResponseStart() > 0);
        var forceFlush = meterProvider.ForceFlush;
        var mockStream = new Mock<ReaderStream>();
        var responses = Channel.CreateUnbounded<(bool HasNext, FromServer? Response)>();
        var handledEvents = Channel.CreateUnbounded<long>();
        SetupResponseStream(mockStream, responses, handledEvents.Writer);
        var deserializer = new Mock<IDeserializer<string>>();
        deserializer.Setup(instance => instance.Deserialize(It.IsAny<byte[]>())).Returns((byte[] data) =>
        {
            Assert.True(ObserveAge() > 0);
            return Encoding.UTF8.GetString(data);
        });
        await using var reader = new ReaderBuilder<string>(CreateDriverFactory(mockStream, readerName))
        {
            ConsumerName = "message-age-consumer",
            ReaderName = readerName,
            Deserializer = deserializer.Object,
            SubscribeSettings = { new SubscribeSettings("/topic"), new SubscribeSettings("/another-topic") }
        }.Build();

        Assert.Equal(0, ObserveAge());
        Assert.Equal(MetricType.DoubleGauge, GetMetric(exportedItems, metricName).MetricType);
        Assert.Equal("s", GetMetric(exportedItems, metricName).Unit);
        await responses.Writer.WriteAsync((true, InitResponse));
        await responses.Writer.WriteAsync((true, StartPartitionSessionRequest()));
        var anotherPartition = StartPartitionSessionRequest(partitionSessionId: 2);
        anotherPartition.StartPartitionSessionRequest.PartitionSession.Path = "/another-topic";
        await responses.Writer.WriteAsync((true, anotherPartition));
        await responses.Writer.WriteAsync((true, ReadResponse("first"u8.ToArray(), "second"u8.ToArray())));
        var anotherTopicResponse = ReadResponse("third"u8.ToArray());
        anotherTopicResponse.ReadResponse.PartitionData[0].PartitionSessionId = 2;
        await responses.Writer.WriteAsync((true, anotherTopicResponse));
        await responses.Writer.WriteAsync((true, ReadResponse()));
        await responses.Writer.WriteAsync((true, StartPartitionSessionRequest(partitionSessionId: 3)));
        for (var expectedEvent = 0; expectedEvent <= 3; expectedEvent++)
        {
            Assert.Equal(expectedEvent, await handledEvents.Reader.ReadAsync().AsTask().WaitAsync(timeout));
        }

        Assert.Equal("first", (await reader.ReadAsync().AsTask().WaitAsync(timeout)).Data);
        Assert.True(ObserveAge() > 0);
        var batch = await reader.ReadBatchAsync().AsTask().WaitAsync(timeout);
        Assert.Equal("second", Assert.Single(batch.Batch).Data);
        Assert.True(ObserveAge() > 0);
        var last = readLastAsBatch
            ? Assert.Single((await reader.ReadBatchAsync().AsTask().WaitAsync(timeout)).Batch)
            : await reader.ReadAsync().AsTask().WaitAsync(timeout);
        Assert.Equal("third", last.Data);
        Assert.Equal("/another-topic", last.Topic);
        Assert.True(ObserveAge() > 0);
        using var cancellation = new CancellationTokenSource();
        var nextRead = reader.ReadAsync(cancellation.Token).AsTask();
        Assert.Equal(0, ObserveAge());
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => nextRead);

        Assert.Equal(0, ObserveAge());
        return;

        double ObserveAge()
        {
            exportedItems.Clear();
            forceFlush();
            var point = Assert.Single(GetReaderPoints(exportedItems, metricName, readerName));
            AssertTags(point, "message-age-consumer", readerName);
            return point.GetGaugeLastValueDouble();
        }
    }

    [Fact]
    public async Task PartitionSessionCount_TracksSessionsAcrossReconnectAndDispose()
    {
        const string consumer = "partition-count-consumer";
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
            ConsumerName = consumer,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();
        string? readerName = null;

        try
        {
            await SendResponse(meterProvider, InitResponse, 0, 0);
            AssertGeneratedReaderName(readerName!);
            Assert.Equal("{session}", GetMetric(exportedItems, PartitionSessionCountMetricName).Unit);
            await SendResponse(meterProvider, StartPartitionSessionRequest(partitionSessionId: 1), 1, 1);
            await SendResponse(meterProvider, StartPartitionSessionRequest(partitionSessionId: 2), 2, 2);
            await SendResponse(meterProvider, StopPartitionSessionRequest(partitionSessionId: 1), -1, 1);
            await SendResponse(meterProvider, StopPartitionSessionRequest(partitionSessionId: 2), -2, 0);
            await responses.Writer.WriteAsync((false, null));
            await SendResponse(meterProvider, InitResponse, 0, 0);
            mockStream.Verify(stream => stream.Write(It.Is<FromClient>(message =>
                message.InitRequest != null && message.InitRequest.ReaderName == "")), Times.Exactly(2));
        }
        finally
        {
            await reader.DisposeAsync();
        }

        var afterDisposeItems = new List<Metric>();
        using var afterDisposeMeterProvider = CreateMeterProvider(afterDisposeItems);
        afterDisposeMeterProvider.ForceFlush();
        Assert.Empty(GetReaderPoints(afterDisposeItems, PartitionSessionCountMetricName, readerName!));
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
            var metric = GetMetric(exportedItems, PartitionSessionCountMetricName);
            var point = Assert.Single(EnumeratePoints(metric),
                point => ToDictionary(point.Tags).GetValueOrDefault("consumer") as string == consumer);
            readerName ??= Assert.IsType<string>(ToDictionary(point.Tags)["reader.name"]);
            Assert.Equal(expectedCount, point.GetGaugeLastValueLong());
            AssertTags(point, consumer, readerName);
        }
    }

    [Fact]
    public async Task CommitOffsetLag_TracksMaximumAcrossPartitions()
    {
        const string readerName = "commit-offset-lag-reader";
        const string consumer = "commit-offset-lag-consumer";
        var timeout = TimeSpan.FromSeconds(5);
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);
        var stream = new Mock<ReaderStream>();
        var responses = Channel.CreateUnbounded<(bool HasNext, FromServer? Response)>();
        var handledEvents = Channel.CreateUnbounded<long>();
        var commitsWritten = Channel.CreateUnbounded<long>();
        SetupResponseStream(stream, responses, handledEvents.Writer, message =>
        {
            if (message.CommitOffsetRequest is not null)
            {
                commitsWritten.Writer.TryWrite(message.CommitOffsetRequest.CommitOffsets[0].PartitionSessionId);
            }
        });
        await using var reader = new ReaderBuilder<string>(
            CreateDriverFactory(stream, "Reader_Commit_Offset_Lag_Metrics"))
        {
            ConsumerName = consumer,
            ReaderName = readerName,
            SubscribeSettings = { new SubscribeSettings("/topic") }
        }.Build();

        await SendResponse(InitResponse, 0);
        await SendResponse(StartPartitionSessionRequest(10), 1);
        await SendResponse(StartPartitionSessionRequest(10, partitionSessionId: 2), 2);
        AssertLag(0);

        await responses.Writer.WriteAsync((true, ReadResponse(14, "first"u8.ToArray())));
        var secondReadResponse = ReadResponse(17, "second"u8.ToArray());
        secondReadResponse.ReadResponse.PartitionData[0].PartitionSessionId = 2;
        await responses.Writer.WriteAsync((true, secondReadResponse));
        var firstMessage = await reader.ReadAsync().AsTask().WaitAsync(timeout);
        var secondMessage = await reader.ReadAsync().AsTask().WaitAsync(timeout);
        var firstCommit = firstMessage.CommitAsync();
        var secondCommit = secondMessage.CommitAsync();
        Assert.Equal(1, await commitsWritten.Reader.ReadAsync().AsTask().WaitAsync(timeout));
        Assert.Equal(2, await commitsWritten.Reader.ReadAsync().AsTask().WaitAsync(timeout));
        AssertLag(8);

        var secondCommitResponse = CommitOffsetResponse(18);
        secondCommitResponse.CommitOffsetResponse.PartitionsCommittedOffsets[0].PartitionSessionId = 2;
        await responses.Writer.WriteAsync((true, secondCommitResponse));
        await secondCommit.WaitAsync(timeout);
        AssertLag(5);

        await responses.Writer.WriteAsync((true, CommitOffsetResponse(15)));
        await firstCommit.WaitAsync(timeout);
        AssertLag(0);

        await SendResponse(StopPartitionSessionRequest(), -1);
        await SendResponse(StopPartitionSessionRequest(partitionSessionId: 2), -2);
        AssertLag(0);

        return;

        async Task SendResponse(FromServer response, long expectedEvent)
        {
            await responses.Writer.WriteAsync((true, response));
            Assert.Equal(expectedEvent, await handledEvents.Reader.ReadAsync().AsTask().WaitAsync(timeout));
        }

        void AssertLag(long expected)
        {
            exportedItems.Clear();
            meterProvider.ForceFlush();
            var metric = GetMetric(exportedItems, CommitOffsetLagMetricName);
            Assert.Equal(MetricType.LongGauge, metric.MetricType);
            Assert.Equal("{message}", metric.Unit);
            var point = Assert.Single(GetReaderPoints(exportedItems, CommitOffsetLagMetricName, readerName));
            Assert.Equal(expected, point.GetGaugeLastValueLong());
            AssertTags(point, consumer, readerName);
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

    private static void AssertGeneratedReaderName(string readerName)
    {
        const string prefix = "reader-";
        Assert.StartsWith(prefix, readerName);
        Assert.True(long.TryParse(readerName.AsSpan(prefix.Length), out var id));
        Assert.True(id > 0);
    }

    private static void SetupResponseStream(
        Mock<ReaderStream> stream,
        Channel<(bool HasNext, FromServer? Response)> responses,
        ChannelWriter<long> handledEvents,
        Action<FromClient>? onWrite = null)
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
                onWrite?.Invoke(message);
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
