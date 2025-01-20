using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.Threading.Channels;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Ado;
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
    private const double FreeBufferCoefficient = 0.2;

    private readonly IDriver _driver;
    private readonly ReaderConfig _config;
    private readonly IDeserializer<TValue> _deserializer;
    private readonly ILogger _logger;
    private readonly GrpcRequestSettings _readerGrpcRequestSettings;

    private readonly Channel<InternalBatchMessage> _receivedMessagesChannel =
        Channel.CreateUnbounded<InternalBatchMessage>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = true,
                AllowSynchronousContinuations = false
            }
        );

    private readonly CancellationTokenSource _disposeCts = new();

    internal Reader(IDriver driver, ReaderConfig config, IDeserializer<TValue> deserializer)
    {
        _driver = driver;
        _config = config;
        _deserializer = deserializer;
        _logger = driver.LoggerFactory.CreateLogger<Reader<TValue>>();
        _readerGrpcRequestSettings = new GrpcRequestSettings { CancellationToken = _disposeCts.Token };

        _ = Initialize();
    }

    public async ValueTask<Message<TValue>> ReadAsync(CancellationToken cancellationToken = default)
    {
        while (await _receivedMessagesChannel.Reader.WaitToReadAsync(cancellationToken))
        {
            if (_receivedMessagesChannel.Reader.TryPeek(out var batchInternalMessage))
            {
                if (!batchInternalMessage.ReaderSession.IsActive)
                {
                    continue;
                }

                if (batchInternalMessage.InternalMessages.TryDequeue(out var message))
                {
                    return message.ToPublicMessage(_deserializer, batchInternalMessage.ReaderSession);
                }

                if (!_receivedMessagesChannel.Reader.TryRead(out _))
                {
                    throw new ReaderException("Detect race condition on ReadAsync operation");
                }
            }
            else
            {
                throw new ReaderException("Detect race condition on ReadAsync operation");
            }
        }

        throw new ReaderException("Reader is disposed");
    }

    public async ValueTask<BatchMessage<TValue>> ReadBatchAsync(CancellationToken cancellationToken = default)
    {
        while (await _receivedMessagesChannel.Reader.WaitToReadAsync(cancellationToken))
        {
            if (!_receivedMessagesChannel.Reader.TryRead(out var batchInternalMessage))
            {
                throw new ReaderException("Detect race condition on ReadBatchAsync operation");
            }

            if (batchInternalMessage.InternalMessages.Count == 0 || !batchInternalMessage.ReaderSession.IsActive)
            {
                continue;
            }

            return new BatchMessage<TValue>(
                batchInternalMessage.InternalMessages
                    .Select(message => message.ToPublicMessage(_deserializer, batchInternalMessage.ReaderSession))
                    .ToImmutableArray(),
                batchInternalMessage.ReaderSession
            );
        }

        throw new ReaderException("Reader is disposed");
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

            var stream = _driver.BidirectionalStreamCall(TopicService.StreamReadMethod, _readerGrpcRequestSettings);

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

            new ReaderSession(
                _config,
                stream,
                initResponse.SessionId,
                Initialize,
                _logger,
                _receivedMessagesChannel.Writer
            ).RunProcessingTopic();
        }
        catch (Driver.TransportException e)
        {
            _logger.LogError(e, "Transport error on executing ReaderSession");

            _ = Task.Run(Initialize, _disposeCts.Token);
        }
    }

    public void Dispose()
    {
        try
        {
            _disposeCts.Cancel();
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
    private readonly CancellationTokenSource _lifecycleReaderSessionCts = new();

    private readonly Channel<CommitSending> _channelCommitSending = Channel.CreateUnbounded<CommitSending>(
        new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true,
            AllowSynchronousContinuations = false
        }
    );

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
        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var commitSending in _channelCommitSending.Reader.ReadAllAsync())
                {
                    if (_partitionSessions.TryGetValue(commitSending.PartitionSessionId, out var partitionSession))
                    {
                        partitionSession.RegisterCommitRequest(commitSending);
                    }
                    else
                    {
                        Logger.LogWarning(
                            "Offset range [{OffsetRange}] is requested to be committed, " +
                            "but PartitionSession[PartitionSessionId={PartitionSessionId}] is already closed",
                            commitSending.OffsetsRange, commitSending.PartitionSessionId);
                    }

                    await Stream.Write(new MessageFromClient
                    {
                        CommitOffsetRequest = new StreamReadMessage.Types.CommitOffsetRequest
                        {
                            CommitOffsets =
                            {
                                new StreamReadMessage.Types.CommitOffsetRequest.Types.PartitionCommitOffset
                                {
                                    Offsets = { commitSending.OffsetsRange },
                                    PartitionSessionId = commitSending.PartitionSessionId
                                }
                            }
                        }
                    });
                }
            }
            catch (Driver.TransportException e)
            {
                Logger.LogError(e, "ReaderSession[{SessionId}] have transport error on Commit", SessionId);

                _lifecycleReaderSessionCts.Cancel();

                ReconnectSession();
            }
        });

        try
        {
            while (await Stream.MoveNextAsync())
            {
                switch (Stream.Current.ServerMessageCase)
                {
                    case ServerMessageOneofCase.ReadResponse:
                        await HandleReadResponse();
                        break;
                    case ServerMessageOneofCase.StartPartitionSessionRequest:
                        await HandleStartPartitionSessionRequest();
                        break;
                    case ServerMessageOneofCase.CommitOffsetResponse:
                        HandleCommitOffsetResponse();
                        break;
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
        catch (Driver.TransportException e)
        {
            Logger.LogError(e, "ReaderSession[{SessionId}] have transport error on processing server messages",
                SessionId);
        }
        finally
        {
            _lifecycleReaderSessionCts.Cancel();

            ReconnectSession();
        }
    }

    private async Task HandleStartPartitionSessionRequest()
    {
        var startPartitionSessionRequest = Stream.Current.StartPartitionSessionRequest;
        var partitionSession = startPartitionSessionRequest.PartitionSession;
        _partitionSessions[partitionSession.PartitionSessionId] = new PartitionSession(
            Logger,
            partitionSession.PartitionSessionId,
            partitionSession.Path,
            partitionSession.PartitionId,
            startPartitionSessionRequest.CommittedOffset
        );

        Logger.LogInformation(
            "ReaderSession[{SessionId}] started PartitionSession[PartitionSessionId={PartitionSessionId}, " +
            "Path={Path}, PartitionId={PartitionId}, CommittedOffset={CommittedOffset}]",
            SessionId, partitionSession.PartitionSessionId, partitionSession.Path,
            partitionSession.PartitionId, startPartitionSessionRequest.CommittedOffset);

        await Stream.Write(new MessageFromClient
        {
            StartPartitionSessionResponse = new StreamReadMessage.Types.StartPartitionSessionResponse
            {
                PartitionSessionId = partitionSession.PartitionSessionId
                /* Simple client doesn't have read_offset or commit_offset settings */
            }
        });
    }

    private void HandleCommitOffsetResponse()
    {
        foreach (var partitionsCommittedOffset in Stream.Current.CommitOffsetResponse.PartitionsCommittedOffsets)
        {
            if (_partitionSessions.TryGetValue(partitionsCommittedOffset.PartitionSessionId,
                    out var partitionSession))
            {
                partitionSession.HandleCommitedOffset(partitionSession.CommitedOffset);
            }
            else
            {
                Logger.LogError(
                    "Received CommitOffsetResponse[CommittedOffset={CommittedOffset}] " +
                    "for unknown PartitionSession[PartitionSessionId={PartitionSessionId}]",
                    partitionsCommittedOffset.CommittedOffset, partitionsCommittedOffset.PartitionSessionId);
            }
        }
    }

    public async Task CommitOffsetRange(OffsetsRange offsetsRange, long partitionId)
    {
        var tcsCommit = new TaskCompletionSource();

        await using var register = _lifecycleReaderSessionCts.Token.Register(() => tcsCommit
            .TrySetException(new YdbException($"ReaderSession[{SessionId}] was deactivated")));

        await _channelCommitSending.Writer.WriteAsync(new CommitSending(offsetsRange, partitionId, tcsCommit));

        await tcsCommit.Task;
    }

    private async Task HandleReadResponse()
    {
        var readResponse = Stream.Current.ReadResponse;

        Interlocked.Add(ref _memoryUsageMaxBytes, -readResponse.BytesSize);

        var bytesSize = readResponse.BytesSize;
        var partitionCount = readResponse.PartitionData.Count;

        for (var partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++)
        {
            var partition = readResponse.PartitionData[partitionIndex];
            var partitionSessionId = partition.PartitionSessionId;
            var approximatelyPartitionBytesSize = CalculateApproximatelyBytesSize(
                bytesSize: bytesSize,
                countParts: partitionCount,
                currentIndex: partitionIndex
            );

            if (_partitionSessions.TryGetValue(partitionSessionId, out var partitionSession))
            {
                var startOffsetBatch = partitionSession.CommitedOffset;
                var endOffsetBatch = partitionSession.CommitedOffset;

                var batchCount = partition.Batches.Count;
                var batch = partition.Batches;

                for (var batchIndex = 0; batchIndex < batchCount; batchIndex++)
                {
                    var approximatelyBatchBytesSize = CalculateApproximatelyBytesSize(
                        bytesSize: approximatelyPartitionBytesSize,
                        countParts: batchCount,
                        currentIndex: batchIndex
                    );

                    var internalBatchMessages = new Queue<InternalMessage>();
                    var messagesCount = batch[batchIndex].MessageData.Count;

                    for (var messageIndex = 0; messageIndex < messagesCount; messageIndex++)
                    {
                        var messageData = batch[batchIndex].MessageData[messageIndex];

                        internalBatchMessages.Enqueue(
                            new InternalMessage(
                                data: messageData.Data,
                                topic: partitionSession.TopicPath,
                                partitionId: partitionSession.PartitionId,
                                producerId: batch[batchIndex].ProducerId,
                                offsetsRange: new OffsetsRange
                                    { Start = partitionSession.PrevEndOffsetMessage, End = messageData.Offset },
                                createdAt: messageData.CreatedAt,
                                metadataItems: messageData.MetadataItems,
                                CalculateApproximatelyBytesSize(
                                    bytesSize: approximatelyBatchBytesSize,
                                    countParts: messagesCount,
                                    currentIndex: messageIndex
                                )
                            )
                        );

                        partitionSession.PrevEndOffsetMessage = endOffsetBatch = messageData.Offset + 1;
                    }

                    await _channelWriter.WriteAsync(
                        new InternalBatchMessage(
                            new OffsetsRange { Start = startOffsetBatch, End = endOffsetBatch },
                            internalBatchMessages,
                            this,
                            approximatelyBatchBytesSize
                        )
                    );
                }
            }
            else
            {
                Logger.LogError(
                    "ReaderSession[{SessionId}]: received PartitionData for unknown(closed?) " +
                    "PartitionSession[{PartitionSessionId}], all messages were skipped!",
                    SessionId, partitionSessionId);

                Interlocked.Add(ref _memoryUsageMaxBytes, approximatelyPartitionBytesSize);
            }
        }
    }

    private static long CalculateApproximatelyBytesSize(long bytesSize, int countParts, int currentIndex)
    {
        return bytesSize / countParts + currentIndex == countParts - 1 ? bytesSize % countParts : 0;
    }

    private class PartitionSession
    {
        private readonly ILogger _logger;
        private readonly ConcurrentQueue<(long EndOffset, TaskCompletionSource TcsCommit)> _waitCommitMessages = new();

        public PartitionSession(
            ILogger logger,
            long partitionSessionId,
            string topicPath,
            long partitionId,
            long commitedOffset)
        {
            _logger = logger;
            PartitionSessionId = partitionSessionId;
            TopicPath = topicPath;
            PartitionId = partitionId;
            CommitedOffset = commitedOffset;
            PrevEndOffsetMessage = commitedOffset;
        }

        // Identifier of partition session. Unique inside one RPC call.
        internal long PartitionSessionId { get; }

        // Topic path of partition
        internal string TopicPath { get; }

        // Partition identifier
        internal long PartitionId { get; }

        // Each offset up to and including (committed_offset - 1) was fully processed.
        internal long CommitedOffset { get; set; }

        internal long PrevEndOffsetMessage { get; set; }

        internal void RegisterCommitRequest(CommitSending commitSending)
        {
            var endOffset = commitSending.OffsetsRange.End;

            if (endOffset <= CommitedOffset)
            {
                commitSending.TcsCommit.SetResult();
            }
            else
            {
                _waitCommitMessages.Enqueue((endOffset, commitSending.TcsCommit));
            }
        }

        internal void HandleCommitedOffset(long commitedOffset)
        {
            if (CommitedOffset >= commitedOffset)
            {
                _logger.LogError(
                    "Received CommitOffsetResponse[CommitedOffset={CommitedOffset}] " +
                    "which is not greater than previous committed offset: {PrevCommitedOffset}",
                    commitedOffset, CommitedOffset);
            }

            CommitedOffset = commitedOffset;

            while (_waitCommitMessages.TryPeek(out var waitCommitTcs) && waitCommitTcs.EndOffset <= commitedOffset)
            {
                _waitCommitMessages.TryDequeue(out _);
                waitCommitTcs.TcsCommit.SetResult();
            }
        }
    }
}
