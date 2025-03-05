using System.Collections.Concurrent;
using System.Threading.Channels;
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
    private readonly GrpcRequestSettings _readerGrpcRequestSettings = new();

    private ReaderSession<TValue>? _currentReaderSession;

    private readonly Channel<InternalBatchMessages<TValue>> _receivedMessagesChannel =
        Channel.CreateUnbounded<InternalBatchMessages<TValue>>(
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

        _ = Initialize();
    }

    public async ValueTask<Message<TValue>> ReadAsync(CancellationToken cancellationToken = default)
    {
        while (await _receivedMessagesChannel.Reader.WaitToReadAsync(cancellationToken))
        {
            if (_receivedMessagesChannel.Reader.TryPeek(out var batchInternalMessage))
            {
                if (batchInternalMessage.TryDequeueMessage(out var message))
                {
                    return message;
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

    public async ValueTask<BatchMessages<TValue>> ReadBatchAsync(CancellationToken cancellationToken = default)
    {
        while (await _receivedMessagesChannel.Reader.WaitToReadAsync(cancellationToken))
        {
            if (!_receivedMessagesChannel.Reader.TryRead(out var batchInternalMessage))
            {
                throw new ReaderException("Detect race condition on ReadBatchAsync operation");
            }

            if (batchInternalMessage.TryPublicBatch(out var batch))
            {
                return batch;
            }
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

                foreach (var id in subscribe.PartitionIds)
                {
                    topicReadSettings.PartitionIds.Add(id);
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

            _currentReaderSession = new ReaderSession<TValue>(
                _config,
                stream,
                initResponse.SessionId,
                Initialize,
                _logger,
                _receivedMessagesChannel.Writer,
                _deserializer
            );
        }
        catch (Driver.TransportException e)
        {
            _logger.LogError(e, "Transport error on executing ReaderSession");

            _ = Task.Run(Initialize, _disposeCts.Token);
        }
    }

    public ValueTask DisposeAsync()
    {
        _receivedMessagesChannel.Writer.TryComplete();
        _disposeCts.Cancel();

        return _currentReaderSession?.DisposeAsync() ?? ValueTask.CompletedTask;
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
internal class ReaderSession<TValue> : TopicSession<MessageFromClient, MessageFromServer>
{
    private const double FreeBufferCoefficient = 0.2;

    private readonly ReaderConfig _readerConfig;
    private readonly ChannelWriter<InternalBatchMessages<TValue>> _channelWriter;
    private readonly CancellationTokenSource _lifecycleReaderSessionCts = new();
    private readonly IDeserializer<TValue> _deserializer;
    private readonly Task _runProcessingStreamResponse;
    private readonly Task _runProcessingStreamRequest;

    private readonly Channel<MessageFromClient> _channelFromClientMessageSending =
        Channel.CreateUnbounded<MessageFromClient>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                AllowSynchronousContinuations = false
            }
        );

    private readonly ConcurrentDictionary<long, PartitionSession> _partitionSessions = new();

    private long _readRequestBytes;

    public ReaderSession(
        ReaderConfig config,
        ReaderStream stream,
        string sessionId,
        Func<Task> initialize,
        ILogger logger,
        ChannelWriter<InternalBatchMessages<TValue>> channelWriter,
        IDeserializer<TValue> deserializer
    ) : base(
        stream,
        logger,
        sessionId,
        initialize
    )
    {
        _readerConfig = config;
        _channelWriter = channelWriter;
        _deserializer = deserializer;

        _runProcessingStreamResponse = RunProcessingStreamResponse();
        _runProcessingStreamRequest = RunProcessingStreamRequest();
    }

    private async Task RunProcessingStreamResponse()
    {
        try
        {
            while (await Stream.MoveNextAsync())
            {
                var messageFromServer = Stream.Current;

                var status = Status.FromProto(messageFromServer.Status, messageFromServer.Issues);

                if (status.IsNotSuccess)
                {
                    Logger.LogError(
                        "ReaderSession[{SessionId}] received unsuccessful status while processing readAck: {Status}",
                        SessionId, status);
                    return;
                }

                switch (messageFromServer.ServerMessageCase)
                {
                    case ServerMessageOneofCase.ReadResponse:
                        await HandleReadResponse(messageFromServer.ReadResponse);
                        break;
                    case ServerMessageOneofCase.StartPartitionSessionRequest:
                        await HandleStartPartitionSessionRequest(messageFromServer.StartPartitionSessionRequest);
                        break;
                    case ServerMessageOneofCase.CommitOffsetResponse:
                        HandleCommitOffsetResponse(messageFromServer.CommitOffsetResponse);
                        break;
                    case ServerMessageOneofCase.PartitionSessionStatusResponse:
                    case ServerMessageOneofCase.UpdateTokenResponse:
                    case ServerMessageOneofCase.StopPartitionSessionRequest:
                        await StopPartitionSessionRequest(messageFromServer.StopPartitionSessionRequest);
                        break;
                    case ServerMessageOneofCase.InitResponse:
                    case ServerMessageOneofCase.None:
                    case ServerMessageOneofCase.UpdatePartitionSession:
                    case ServerMessageOneofCase.EndPartitionSession:
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
        catch (OperationCanceledException)
        {
            Logger.LogInformation("");
        }
        finally
        {
            ReconnectSession();

            _lifecycleReaderSessionCts.Cancel();
        }
    }

    private async Task RunProcessingStreamRequest()
    {
        try
        {
            await foreach (var messageFromClient in _channelFromClientMessageSending.Reader.ReadAllAsync())
            {
                await SendMessage(messageFromClient);
            }
        }
        catch (Driver.TransportException e)
        {
            Logger.LogError(e, "ReaderSession[{SessionId}] have transport error on Write", SessionId);

            ReconnectSession();
            
            _lifecycleReaderSessionCts.Cancel();
        }
    }

    internal async void TryReadRequestBytes(long bytes)
    {
        var readRequestBytes = Interlocked.Add(ref _readRequestBytes, bytes);

        if (readRequestBytes < FreeBufferCoefficient * _readerConfig.MemoryUsageMaxBytes)
        {
            return;
        }

        if (Interlocked.CompareExchange(ref _readRequestBytes, 0, readRequestBytes) == readRequestBytes)
        {
            await _channelFromClientMessageSending.Writer.WriteAsync(new MessageFromClient
                { ReadRequest = new StreamReadMessage.Types.ReadRequest { BytesSize = readRequestBytes } });
        }
    }

    private async Task HandleStartPartitionSessionRequest(
        StreamReadMessage.Types.StartPartitionSessionRequest startPartitionSessionRequest)
    {
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
            "Path=\"{Path}\", PartitionId={PartitionId}, CommittedOffset={CommittedOffset}]",
            SessionId, partitionSession.PartitionSessionId, partitionSession.Path,
            partitionSession.PartitionId, startPartitionSessionRequest.CommittedOffset);

        await _channelFromClientMessageSending.Writer.WriteAsync(new MessageFromClient
        {
            StartPartitionSessionResponse = new StreamReadMessage.Types.StartPartitionSessionResponse
            {
                PartitionSessionId = partitionSession.PartitionSessionId
                /* Simple client doesn't have read_offset or commit_offset settings */
            }
        });
    }

    private void HandleCommitOffsetResponse(StreamReadMessage.Types.CommitOffsetResponse commitOffsetResponse)
    {
        foreach (var partitionsCommittedOffset in commitOffsetResponse.PartitionsCommittedOffsets)
        {
            if (_partitionSessions.TryGetValue(partitionsCommittedOffset.PartitionSessionId,
                    out var partitionSession))
            {
                partitionSession.HandleCommitedOffset(partitionsCommittedOffset.CommittedOffset);
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

    private async Task StopPartitionSessionRequest(
        StreamReadMessage.Types.StopPartitionSessionRequest stopPartitionSessionRequest)
    {
        if (_partitionSessions.TryRemove(stopPartitionSessionRequest.PartitionSessionId, out var partitionSession))
        {
            Logger.LogInformation("ReaderSession[{SessionId}] has stopped PartitionSession" +
                                  "[PartitionSessionId={PartitionSessionId}, Path={Path}, PartitionId={PartitionId}, " +
                                  "CommittedOffset={CommittedOffset}] with GracefulFlag = {Graceful}.",
                SessionId, stopPartitionSessionRequest.PartitionSessionId, partitionSession.TopicPath,
                partitionSession.PartitionId, stopPartitionSessionRequest.CommittedOffset,
                stopPartitionSessionRequest.Graceful);

            partitionSession.Stop(stopPartitionSessionRequest.CommittedOffset);

            if (stopPartitionSessionRequest.Graceful)
            {
                await _channelFromClientMessageSending.Writer.WriteAsync(new MessageFromClient
                {
                    StopPartitionSessionResponse = new StreamReadMessage.Types.StopPartitionSessionResponse
                        { PartitionSessionId = partitionSession.PartitionSessionId }
                });
            }
        }
        else
        {
            Logger.LogError(
                "Received StopPartitionSessionRequest[PartitionSessionId={PartitionSessionId}] for unknown PartitionSession",
                stopPartitionSessionRequest.PartitionSessionId);
        }
    }

    public async Task CommitOffsetRange(OffsetsRange offsetsRange, long partitionSessionId)
    {
        var tcsCommit = new TaskCompletionSource();

        await using var register = _lifecycleReaderSessionCts.Token.Register(
            () => tcsCommit.TrySetException(new ReaderException($"ReaderSession[{SessionId}] was deactivated"))
        );

        var commitSending = new CommitSending(offsetsRange, tcsCommit);

        if (_partitionSessions.TryGetValue(partitionSessionId, out var partitionSession))
        {
            partitionSession.RegisterCommitRequest(commitSending);

            await _channelFromClientMessageSending.Writer.WriteAsync(new MessageFromClient
                {
                    CommitOffsetRequest = new StreamReadMessage.Types.CommitOffsetRequest
                    {
                        CommitOffsets =
                        {
                            new StreamReadMessage.Types.CommitOffsetRequest.Types.PartitionCommitOffset
                            {
                                Offsets = { commitSending.OffsetsRange },
                                PartitionSessionId = partitionSessionId
                            }
                        }
                    }
                }
            );
        }
        else
        {
            Logger.LogWarning("Offset range [{OffsetRange}] is requested to be committed, " +
                              "but PartitionSession[PartitionSessionId={PartitionSessionId}] is already closed",
                commitSending.OffsetsRange, partitionSessionId);

            Utils.SetPartitionClosedException(commitSending, partitionSessionId);
        }

        await tcsCommit.Task;
    }

    private async Task HandleReadResponse(StreamReadMessage.Types.ReadResponse readResponse)
    {
        var bytesSize = readResponse.BytesSize;
        var partitionCount = readResponse.PartitionData.Count;

        for (var partitionIndex = 0; partitionIndex < partitionCount; partitionIndex++)
        {
            var partition = readResponse.PartitionData[partitionIndex];
            var partitionSessionId = partition.PartitionSessionId;
            var approximatelyPartitionBytesSize = Utils.CalculateApproximatelyBytesSize(
                bytesSize: bytesSize,
                countParts: partitionCount,
                currentIndex: partitionIndex
            );

            if (_partitionSessions.TryGetValue(partitionSessionId, out var partitionSession))
            {
                var batchCount = partition.Batches.Count;
                var batches = partition.Batches;

                for (var batchIndex = 0; batchIndex < batchCount; batchIndex++)
                {
                    await _channelWriter.WriteAsync(
                        new InternalBatchMessages<TValue>(
                            batches[batchIndex],
                            partitionSession,
                            this,
                            Utils.CalculateApproximatelyBytesSize(
                                bytesSize: approximatelyPartitionBytesSize,
                                countParts: batchCount,
                                currentIndex: batchIndex
                            ),
                            _deserializer
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
            }
        }
    }

    protected override MessageFromClient GetSendUpdateTokenRequest(string token)
    {
        return new MessageFromClient
        {
            UpdateTokenRequest = new UpdateTokenRequest
            {
                Token = token
            }
        };
    }

    public override async ValueTask DisposeAsync()
    {
        _channelFromClientMessageSending.Writer.Complete();

        try
        {
            await _runProcessingStreamRequest;
            await Stream.RequestStreamComplete();
            await _runProcessingStreamResponse; // waiting all ack's commits
            
            _lifecycleReaderSessionCts.Cancel();
        }
        finally
        {
            Stream.Dispose();
        }
    }
}
