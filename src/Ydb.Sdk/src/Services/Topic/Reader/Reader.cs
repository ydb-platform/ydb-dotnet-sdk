using System.Collections.Concurrent;
using System.Net.Sockets;
using System.Threading.Channels;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using Ydb.Topic;
using Ydb.Topic.V1;
using static Ydb.Topic.StreamReadMessage.Types.FromServer;

namespace Ydb.Sdk.Services.Topic.Reader;

using MessageFromClient = StreamReadMessage.Types.FromClient;
using MessageFromServer = StreamReadMessage.Types.FromServer;
using ReaderStream = IBidirectionalStream<
    StreamReadMessage.Types.FromClient,
    StreamReadMessage.Types.FromServer
>;

internal class Reader<TValue> : IReader<TValue>
{
    private readonly IDriver _driver;
    private readonly ReaderConfig _config;
    private readonly IDeserializer<TValue> _deserializer;
    private readonly ILogger _logger;

    private readonly Channel<InternalBatchMessage> _receivedMessagesChannel =
        Channel.CreateUnbounded<InternalBatchMessage>();

    private readonly CancellationTokenSource _disposeCts = new();

    private volatile ReaderSession? _readerSession;

    internal Reader(IDriver driver, ReaderConfig config, IDeserializer<TValue> deserializer)
    {
        _driver = driver;
        _config = config;
        _deserializer = deserializer;
        _logger = driver.LoggerFactory.CreateLogger<Reader<TValue>>();

        _ = Initialize();
    }

    public ValueTask<Message<TValue>> ReadAsync(CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    public ValueTask<IReadOnlyList<Message<TValue>>> ReadBatchAsync(CancellationToken cancellationToken = default)
    {
        throw new NotImplementedException();
    }

    private async Task Initialize()
    {
        try
        {
            if (_disposeCts.IsCancellationRequested)
            {
                _logger.LogWarning("Reader writer is canceled because it has been disposed");

                return;
            }

            _logger.LogInformation("Reader session initialization started. ReaderConfig: {ReaderConfig}", _config);

            var stream = _driver.BidirectionalStreamCall(
                TopicService.StreamReadMethod,
                GrpcRequestSettings.DefaultInstance
            );

            var initRequest = new StreamReadMessage.Types.InitRequest();
            if (_config.ConsumerName != null)
            {
                initRequest.Consumer = _config.ConsumerName;
            }

            if (_config.ReaderName != null)
            {
                initRequest.ReaderName = _config.ReaderName;
            }

            foreach (var subscribe in _config.SubscribeSettings)
            {
                var topicReadSettings = new StreamReadMessage.Types.InitRequest.Types.TopicReadSettings
                {
                    Path = subscribe.TopicPath
                };

                if (subscribe.MaxLag != null)
                {
                    topicReadSettings.MaxLag = Duration.FromTimeSpan(subscribe.MaxLag.Value);
                }

                if (subscribe.ReadFrom != null)
                {
                    topicReadSettings.ReadFrom = Timestamp.FromDateTime(subscribe.ReadFrom.Value);
                }

                initRequest.TopicsReadSettings.Add(topicReadSettings);
            }

            _logger.LogDebug("Sending initialization request for the read stream: {InitRequest}", initRequest);

            await stream.Write(new MessageFromClient { InitRequest = initRequest });
            if (!await stream.MoveNextAsync())
            {
                _logger.LogError("Stream unexpectedly closed by YDB server. Current InitRequest: {InitRequest}",
                    initRequest);

                _ = Task.Run(Initialize, _disposeCts.Token);

                return;
            }

            var receivedInitMessage = stream.Current;

            var status = Status.FromProto(receivedInitMessage.Status, receivedInitMessage.Issues);

            if (status.IsNotSuccess)
            {
                if (RetrySettings.DefaultInstance.GetRetryRule(status.StatusCode).Policy != RetryPolicy.None)
                {
                    _logger.LogError("Reader initialization failed to start. Reason: {Status}", status);

                    _ = Task.Run(Initialize, _disposeCts.Token);
                }
                else
                {
                    _logger.LogCritical("Reader initialization failed to start. Reason: {Status}", status);

                    _receivedMessagesChannel.Writer.Complete(new ReaderException("Initialization failed", status));
                }

                return;
            }

            var initResponse = receivedInitMessage.InitResponse;

            _logger.LogDebug("Received a response for the initialization request on the read stream: {InitResponse}",
                initResponse);

            await stream.Write(new MessageFromClient
            {
                ReadRequest = new StreamReadMessage.Types.ReadRequest { BytesSize = _config.MemoryUsageMaxBytes }
            });

            _readerSession = new ReaderSession(
                _config,
                stream,
                initResponse.SessionId,
                Initialize,
                _logger,
                _receivedMessagesChannel.Writer
            );
            _readerSession.RunProcessingTopic();
        }
        catch (Driver.TransportException e)
        {
            _logger.LogError(e, "Transport error on executing ReaderSession");

            _ = Task.Run(Initialize, _disposeCts.Token);
        }
    }

    private void ClearChannelAsync()
    {
        while (_receivedMessagesChannel.Reader.TryRead(out _))
        {
            /* Do nothing, simple read */
        }
    }

    public void Dispose()
    {
        try
        {
            _disposeCts.Cancel();
            
            _readerSession?.Dispose();
        }
        finally
        {
            _disposeCts.Dispose();
        }
    }
}

/// <summary>
/// Server and client each keep track of total bytes size of all ReadResponses.
/// When client is ready to receive N more bytes in responses (to increment possible total by N),
/// it sends a ReadRequest with bytes_size = N.
/// bytes_size value must be positive.
/// So in expression 'A = (sum of bytes_size in all ReadRequests) - (sum of bytes_size in all ReadResponses)'
///   server will keep A (available size for responses) non-negative.
/// But there is an exception. If server receives ReadRequest, and the first message in response exceeds A -
/// then it will still be delivered, and A will become negative until enough additional ReadRequests.
///
/// Example:
/// 1) Let client have 200 bytes buffer. It sends ReadRequest with bytes_size = 200;
/// 2) Server may return one ReadResponse with bytes_size = 70 and then another 80 bytes response;
///    now client buffer has 50 free bytes, server is free to send up to 50 bytes in responses.
/// 3) Client processes 100 bytes from buffer, now buffer free space is 150 bytes,
///    so client sends ReadRequest with bytes_size = 100;
/// 4) Server is free to send up to 50 + 100 = 150 bytes. But the next read message is too big,
///    and it sends 160 bytes ReadResponse.
/// 5) Let's assume client somehow processes it, and its 200 bytes buffer is free again.
///    It should account for excess 10 bytes and send ReadRequest with bytes_size = 210.
/// </summary>
internal class ReaderSession : TopicSession<MessageFromClient, MessageFromServer>
{
    private readonly ChannelWriter<InternalBatchMessage> _channelWriter;
    private readonly ConcurrentDictionary<long, PartitionSession> _partitionSessions = new();

    private long _memoryUsageMaxBytes;

    public ReaderSession(
        ReaderConfig config,
        ReaderStream stream,
        string sessionId,
        Func<Task> initialize,
        ILogger logger,
        ChannelWriter<InternalBatchMessage> channelWriter
    ) : base(
        stream,
        logger,
        sessionId,
        initialize
    )
    {
        _channelWriter = channelWriter;
        _memoryUsageMaxBytes = config.MemoryUsageMaxBytes;
    }

    public async void RunProcessingTopic()
    {
        while (await Stream.MoveNextAsync())
        {
            switch (Stream.Current.ServerMessageCase)
            {
                case ServerMessageOneofCase.ReadResponse:
                    await HandleReadResponse();
                    break;
                case ServerMessageOneofCase.StartPartitionSessionRequest:
                    HandleStartPartitionSessionRequest();
                    break;
                case ServerMessageOneofCase.CommitOffsetResponse:
                case ServerMessageOneofCase.PartitionSessionStatusResponse:
                case ServerMessageOneofCase.UpdateTokenResponse:
                case ServerMessageOneofCase.StopPartitionSessionRequest:
                case ServerMessageOneofCase.InitResponse:
                case ServerMessageOneofCase.None:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }

    private async Task HandleReadResponse()
    {
        var readResponse = Stream.Current.ReadResponse;

        Interlocked.Add(ref _memoryUsageMaxBytes, -readResponse.BytesSize);

        foreach (var partition in readResponse.PartitionData)
        {
            var partitionSessionId = partition.PartitionSessionId;

            if (_partitionSessions.TryGetValue(partitionSessionId, out var partitionSession))
            {
                var internalBatchMessages = new Queue<InternalMessage>();
                var startOffsetBatch = partitionSession.CommitedOffset;
                var endOffsetBatch = partitionSession.CommitedOffset;
                foreach (var batch in partition.Batches)
                {
                    foreach (var messageData in batch.MessageData)
                    {
                        internalBatchMessages.Enqueue(
                            new InternalMessage(
                                messageData.Data,
                                new OffsetsRange { Start = partitionSession.CommitedOffset, End = messageData.Offset },
                                messageData.CreatedAt,
                                messageData.MetadataItems
                            ));

                        partitionSession.CommitedOffset = endOffsetBatch = messageData.Offset + 1;
                    }
                }

                await _channelWriter.WriteAsync(new InternalBatchMessage(
                    new OffsetsRange { Start = startOffsetBatch, End = endOffsetBatch }, internalBatchMessages)
                );
            }
            else
            {
                Logger.LogCritical(
                    "ReaderSession[{SessionId}]: received PartitionData for unknown(closed?) " +
                    "PartitionSession[{PartitionSessionId}], all messages were skipped!",
                    SessionId, partitionSessionId);
            }
        }
    }

    private void HandleStartPartitionSessionRequest()
    {
        var startPartitionSessionRequest = Stream.Current.StartPartitionSessionRequest;
        var partitionSession = startPartitionSessionRequest.PartitionSession;

        _partitionSessions[partitionSession.PartitionSessionId] = new PartitionSession(
            partitionSession.PartitionSessionId,
            partitionSession.Path,
            partitionSession.PartitionId,
            startPartitionSessionRequest.CommittedOffset
        );
    }

    private class PartitionSession
    {
        public PartitionSession(
            long partitionSessionId,
            string topicPath,
            long partitionId,
            long commitedOffset)
        {
            PartitionSessionId = partitionSessionId;
            TopicPath = topicPath;
            PartitionId = partitionId;
            CommitedOffset = commitedOffset;
        }

        // Identifier of partition session. Unique inside one RPC call.
        internal long PartitionSessionId { get; }

        // Topic path of partition
        internal string TopicPath { get; }

        // Partition identifier
        internal long PartitionId { get; }

        // Each offset up to and including (committed_offset - 1) was fully processed.
        internal long CommitedOffset { get; set; }
    }
}

internal class InternalMessage
{
    public InternalMessage(
        ByteString data,
        OffsetsRange offsetsRange,
        Timestamp createAt,
        RepeatedField<MetadataItem> metadataItems)
    {
        Data = data;
        OffsetsRange = offsetsRange;
        CreateAt = createAt;
        MetadataItems = metadataItems;
    }

    public ByteString Data { get; }

    public OffsetsRange OffsetsRange { get; }

    public Timestamp CreateAt { get; }

    public RepeatedField<MetadataItem> MetadataItems { get; }
}

internal class InternalBatchMessage
{
    public InternalBatchMessage(OffsetsRange batchOffsetsRange, Queue<InternalMessage> internalMessages)
    {
        BatchOffsetsRange = batchOffsetsRange;
        InternalMessages = internalMessages;
    }

    public OffsetsRange BatchOffsetsRange { get; }

    public Queue<InternalMessage> InternalMessages { get; }
}
