using System.Diagnostics.Metrics;
using System.Threading.Channels;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Moq;
using Ydb.Topic;

namespace Ydb.Sdk.Topic.Tests;

using FromServer = StreamReadMessage.Types.FromServer;
using FromClient = StreamReadMessage.Types.FromClient;
using ReaderStream = IBidirectionalStream<StreamReadMessage.Types.FromClient, StreamReadMessage.Types.FromServer>;

internal static class ReaderTestUtils
{
    private const string PartitionSessionCountMetricName = "ydb.topic.reader.partition_session.count";

    internal static FromServer InitResponse { get; } = new()
    {
        Status = StatusIds.Types.StatusCode.Success,
        InitResponse = new StreamReadMessage.Types.InitResponse { SessionId = "SessionId" }
    };

    internal static FromServer StartPartitionSessionRequest(int committedOffset = 0, long partitionSessionId = 1) =>
        new()
        {
            Status = StatusIds.Types.StatusCode.Success,
            StartPartitionSessionRequest = new StreamReadMessage.Types.StartPartitionSessionRequest
            {
                CommittedOffset = committedOffset,
                PartitionOffsets = new OffsetsRange { Start = committedOffset, End = committedOffset + 1000 },
                PartitionSession = new StreamReadMessage.Types.PartitionSession
                { Path = "/topic", PartitionId = partitionSessionId, PartitionSessionId = partitionSessionId }
            }
        };

    internal static FromServer StopPartitionSessionRequest(long partitionSessionId = 1) => new()
    {
        Status = StatusIds.Types.StatusCode.Success,
        StopPartitionSessionRequest = new StreamReadMessage.Types.StopPartitionSessionRequest
        {
            PartitionSessionId = partitionSessionId,
            Graceful = true
        }
    };

    internal static FromServer ReadResponse(params byte[][] messages) => ReadResponse(0, messages);

    internal static FromServer ReadResponse(int committedOffset, params byte[][] messages)
    {
        var batch = new StreamReadMessage.Types.ReadResponse.Types.Batch { ProducerId = "ProducerId" };

        foreach (var message in messages)
        {
            batch.MessageData.Add(new StreamReadMessage.Types.ReadResponse.Types.MessageData
            {
                Data = ByteString.CopyFrom(message),
                Offset = committedOffset++,
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

    internal static FromServer CommitOffsetResponse(int committedOffset = 1) => new()
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

    internal static IDriverFactoryMock CreateDriverFactory(Mock<ReaderStream> stream, string name)
    {
        var driver = new Mock<IDriver>();
        driver.Setup(mock => mock.BidirectionalStreamCall(
            It.IsAny<Method<FromClient, FromServer>>(),
            It.IsAny<GrpcRequestSettings>())).ReturnsAsync(stream.Object);
        driver.Setup(mock => mock.DisposeAsync())
            .Callback(() => driver.Setup(mock => mock.IsDisposed).Returns(true));
        driver.Setup(mock => mock.LoggerFactory).Returns(Utils.LoggerFactory);
        return new IDriverFactoryMock(driver, name);
    }

    internal static void SetupResponseStream(
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

    internal static MeterListener CreatePartitionSessionCountListener(
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
            if (metricTags.Any(tag => tag.Key == "endpoint" && Equals(tag.Value, endpoint))
                && (consumer is null
                    || metricTags.Any(tag => tag.Key == "consumer" && Equals(tag.Value, consumer))))
            {
                measurements.Add(new Measurement(instrument.Name, value, metricTags));
            }
        });
        listener.Start();
        return listener;
    }
}

internal sealed record Measurement(
    string InstrumentName,
    long Value,
    KeyValuePair<string, object?>[] Tags);
