using System.Collections.Concurrent;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using Ydb.Topic;
using Ydb.Topic.V1;

namespace Ydb.Sdk.Services.Topic.Writer;

using MessageData = StreamWriteMessage.Types.WriteRequest.Types.MessageData;
using MessageFromClient = StreamWriteMessage.Types.FromClient;
using MessageFromServer = StreamWriteMessage.Types.FromServer;
using WriterStream = IBidirectionalStream<
    StreamWriteMessage.Types.FromClient,
    StreamWriteMessage.Types.FromServer
>;

internal class Writer<TValue> : IWriter<TValue>
{
    private readonly IDriver _driver;
    private readonly WriterConfig _config;
    private readonly ILogger<Writer<TValue>> _logger;
    private readonly ISerializer<TValue> _serializer;
    private readonly GrpcRequestSettings _writerGrpcRequestSettings = new();
    private readonly ConcurrentQueue<MessageSending> _toSendBuffer = new();
    private readonly ConcurrentQueue<MessageSending> _inFlightMessages = new();
    private readonly CancellationTokenSource _disposeCts = new();
    private readonly SemaphoreSlim _sendInFlightMessagesSemaphoreSlim = new(1);

    private volatile TaskCompletionSource _tcsWakeUp = new();
    private volatile TaskCompletionSource _tcsBufferAvailableEvent = new();
    private volatile IWriteSession _session = null!;
    private volatile int _limitBufferMaxSize;
    private volatile bool _isStopped;

    internal Writer(IDriver driver, WriterConfig config, ISerializer<TValue> serializer)
    {
        _driver = driver;
        _config = config;
        _logger = driver.LoggerFactory.CreateLogger<Writer<TValue>>();
        _serializer = serializer;
        _limitBufferMaxSize = config.BufferMaxSize;

        StartWriteWorker();
    }

    public Task<WriteResult> WriteAsync(TValue data, CancellationToken cancellationToken)
    {
        return WriteAsync(new Message<TValue>(data), cancellationToken);
    }

    public async Task<WriteResult> WriteAsync(Message<TValue> message, CancellationToken cancellationToken)
    {
        TaskCompletionSource<WriteResult> tcs = new();
        await using var registrationUserCancellationTokenRegistration = cancellationToken.Register(
            () => tcs.TrySetCanceled(), useSynchronizationContext: false
        );
        await using var writerDisposedCancellationTokenRegistration = _disposeCts.Token.Register(
            () => tcs.TrySetException(new WriterException($"Writer[{_config}] is disposed")),
            useSynchronizationContext: false
        );

        byte[] data;
        try
        {
            data = _serializer.Serialize(message.Data);
        }
        catch (Exception e)
        {
            throw new WriterException("Error when serializing message data", e);
        }

        var messageData = new MessageData
        {
            Data = ByteString.CopyFrom(data),
            CreatedAt = Timestamp.FromDateTime(message.Timestamp.ToUniversalTime()),
            UncompressedSize = data.Length
        };
        foreach (var metadata in message.Metadata)
        {
            messageData.MetadataItems.Add(new MetadataItem
                { Key = metadata.Key, Value = ByteString.CopyFrom(metadata.Value) });
        }

        while (true)
        {
            var curLimitBufferSize = _limitBufferMaxSize;

            if ( // sending one biggest message anyway
                (curLimitBufferSize == _config.BufferMaxSize && data.Length > curLimitBufferSize)
                || curLimitBufferSize >= data.Length)
            {
                if (Interlocked.CompareExchange(ref _limitBufferMaxSize,
                        curLimitBufferSize - data.Length, curLimitBufferSize) == curLimitBufferSize)
                {
                    _toSendBuffer.Enqueue(
                        new MessageSending(messageData, tcs, writerDisposedCancellationTokenRegistration)
                    );
                    WakeUpWorker();

                    break;
                }

                // Next try on race condition
                continue;
            }

            _logger.LogWarning(
                "Buffer overflow: the data size [{DataLength}] exceeds the current buffer limit ({CurLimitBufferSize}) [BufferMaxSize = {BufferMaxSize}]",
                data.Length, curLimitBufferSize, _config.BufferMaxSize);

            try
            {
                await WaitBufferAvailable(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                throw new WriterException("Buffer overflow");
            }
        }

        try
        {
            var writeResult = await tcs.Task;

            return writeResult;
        }
        finally
        {
            Interlocked.Add(ref _limitBufferMaxSize, data.Length);

            _tcsBufferAvailableEvent.TrySetResult();
        }
    }

    private async Task WaitBufferAvailable(CancellationToken cancellationToken)
    {
        var tcsBufferAvailableEvent = _tcsBufferAvailableEvent;

        await tcsBufferAvailableEvent.Task.WaitAsync(cancellationToken);

        Interlocked.CompareExchange(
            ref _tcsBufferAvailableEvent,
            new TaskCompletionSource(),
            tcsBufferAvailableEvent
        );
    }

    private async void StartWriteWorker()
    {
        await Initialize();

        try
        {
            while (!_disposeCts.Token.IsCancellationRequested)
            {
                await _tcsWakeUp.Task.WaitAsync(_disposeCts.Token);
                _tcsWakeUp = new TaskCompletionSource();

                if (_toSendBuffer.IsEmpty)
                {
                    continue;
                }

                await _sendInFlightMessagesSemaphoreSlim.WaitAsync(_disposeCts.Token);
                try
                {
                    if (_session.IsActive)
                    {
                        await _session.Write(_toSendBuffer);
                    }
                }
                finally
                {
                    _sendInFlightMessagesSemaphoreSlim.Release();
                }
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("WriteWorker[{WriterConfig}] is disposed", _config);
        }
    }

    private void WakeUpWorker()
    {
        _tcsWakeUp.TrySetResult();
    }

    private async Task Initialize()
    {
        _session = DummyWriterSession.Instance;

        try
        {
            if (_isStopped)
            {
                _logger.LogDebug("Initialize Writer[{WriterConfig}] is stopped because it has been disposed", _config);

                return;
            }

            _logger.LogInformation("Writer session initialization started. WriterConfig: {WriterConfig}", _config);

            var stream =
                await _driver.BidirectionalStreamCall(TopicService.StreamWriteMethod, _writerGrpcRequestSettings);

            var initRequest = new StreamWriteMessage.Types.InitRequest { Path = _config.TopicPath };
            if (_config.ProducerId != null)
            {
                initRequest.ProducerId = _config.ProducerId;
            }

            if (_config.PartitionId != null)
            {
                initRequest.PartitionId = _config.PartitionId.Value;
            }

            _logger.LogDebug("Sending initialization request for the write stream: {InitRequest}", initRequest);

            await stream.Write(new MessageFromClient { InitRequest = initRequest });
            if (!await stream.MoveNextAsync())
            {
                _logger.LogError("Stream unexpectedly closed by YDB server. Current InitRequest: {initRequest}",
                    initRequest);

                _ = Task.Run(Initialize);

                return;
            }

            var receivedInitMessage = stream.Current;

            var status = Status.FromProto(receivedInitMessage.Status, receivedInitMessage.Issues);

            if (status.IsNotSuccess)
            {
                if (RetrySettings.DefaultInstance.GetRetryRule(status.StatusCode).Policy != RetryPolicy.None)
                {
                    _logger.LogError("Writer initialization failed to start. Reason: {Status}", status);

                    _ = Task.Run(Initialize);
                }
                else
                {
                    _logger.LogCritical("Writer initialization failed to start. Reason: {Status}", status);

                    _session = new NotStartedWriterSession("Initialization failed", status);
                }

                return;
            }

            var initResponse = receivedInitMessage.InitResponse;

            _logger.LogDebug("Received a response for the initialization request on the writer stream: {InitResponse}",
                initResponse);

            if (initResponse.SupportedCodecs != null &&
                !initResponse.SupportedCodecs.Codecs.Contains((int)_config.Codec))
            {
                _logger.LogCritical(
                    "Writer initialization failed to start. Reason: topic[Path=\"{TopicPath}\"] is not supported codec {Codec}",
                    _config.TopicPath, _config.Codec);

                _session = new NotStartedWriterSession(
                    $"Topic[Path=\"{_config.TopicPath}\"] is not supported codec: {_config.Codec}");
                return;
            }

            await _sendInFlightMessagesSemaphoreSlim.WaitAsync();
            try
            {
                var copyInFlightMessages = new ConcurrentQueue<MessageSending>();
                var lastSeqNo = initResponse.LastSeqNo;

                while (_inFlightMessages.TryDequeue(out var sendData))
                {
                    if (lastSeqNo >= sendData.MessageData.SeqNo)
                    {
                        _logger.LogWarning(
                            "Message[SeqNo={SeqNo}] has been skipped because its sequence number " +
                            "is less than or equal to the last processed server's SeqNo[{LastSeqNo}]",
                            sendData.MessageData.SeqNo, lastSeqNo);

                        sendData.Tcs.TrySetResult(WriteResult.Skipped);

                        continue;
                    }


                    // Calculate the next sequence number from the calculated previous messages.
                    lastSeqNo = Math.Max(lastSeqNo, sendData.MessageData.SeqNo);

                    copyInFlightMessages.Enqueue(sendData);
                }

                var newSession = new WriterSession(
                    config: _config,
                    stream: stream,
                    lastSeqNo: lastSeqNo,
                    sessionId: initResponse.SessionId,
                    initialize: Initialize,
                    await stream.AuthToken,
                    logger: _logger,
                    inFlightMessages: _inFlightMessages
                );

                if (!copyInFlightMessages.IsEmpty)
                {
                    await newSession.Write(copyInFlightMessages); // retry prev in flight messages    
                }

                _session = newSession;
                WakeUpWorker(); // attempt send buffer     
            }
            finally
            {
                _sendInFlightMessagesSemaphoreSlim.Release();
            }
        }
        catch (Driver.TransportException e)
        {
            _logger.LogError(e, "Transport error on creating WriterSession");

            _ = Task.Run(Initialize);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Initialize writer is canceled because it has been disposed");
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposeCts.IsCancellationRequested)
        {
            return;
        }

        await _sendInFlightMessagesSemaphoreSlim.WaitAsync();
        try
        {
            _logger.LogDebug("Signaling cancellation token to stop writing new messages");

            _disposeCts.Cancel();
        }
        finally
        {
            _sendInFlightMessagesSemaphoreSlim.Release();
        }

        _logger.LogDebug("Writer[{WriterConfig}] is waiting for all in-flight messages to complete...", _config);

        foreach (var inFlightMessage in _inFlightMessages)
        {
            try
            {
                await inFlightMessage.Tcs.Task;
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Error occurred while waiting for in-flight message SeqNo: {SeqNo}",
                    inFlightMessage.MessageData.SeqNo);
            }
        }

        _isStopped = true;

        await _session.DisposeAsync();

        _logger.LogInformation("Writer[{WriterConfig}] is disposed", _config);
    }
}

internal record MessageSending(
    MessageData MessageData,
    TaskCompletionSource<WriteResult> Tcs,
    CancellationTokenRegistration DisposedCtr
);

internal interface IWriteSession : IAsyncDisposable
{
    Task Write(ConcurrentQueue<MessageSending> toSendBuffer);

    bool IsActive { get; }
}

internal class NotStartedWriterSession : IWriteSession
{
    private readonly WriterException _reasonException;

    public NotStartedWriterSession(string reasonExceptionMessage)
    {
        _reasonException = new WriterException(reasonExceptionMessage);
    }

    public NotStartedWriterSession(string reasonExceptionMessage, Status status)
    {
        _reasonException = new WriterException(reasonExceptionMessage, status);
    }

    public Task Write(ConcurrentQueue<MessageSending> toSendBuffer)
    {
        while (toSendBuffer.TryDequeue(out var messageSending))
        {
            messageSending.Tcs.TrySetException(_reasonException);
        }

        return Task.CompletedTask;
    }

    public bool IsActive => true;

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}

internal class DummyWriterSession : IWriteSession
{
    internal static readonly DummyWriterSession Instance = new();

    private DummyWriterSession()
    {
    }

    public Task Write(ConcurrentQueue<MessageSending> toSendBuffer)
    {
        return Task.CompletedTask;
    }

    public bool IsActive => false;

    public ValueTask DisposeAsync()
    {
        return ValueTask.CompletedTask;
    }
}

internal class WriterSession : TopicSession<MessageFromClient, MessageFromServer>, IWriteSession
{
    private readonly WriterConfig _config;
    private readonly ConcurrentQueue<MessageSending> _inFlightMessages;
    private readonly Task _processingResponseStream;

    private long _seqNum;

    public WriterSession(
        WriterConfig config,
        WriterStream stream,
        long lastSeqNo,
        string sessionId,
        Func<Task> initialize,
        string? lastToken,
        ILogger logger,
        ConcurrentQueue<MessageSending> inFlightMessages
    ) : base(
        stream,
        logger,
        sessionId,
        initialize,
        lastToken
    )
    {
        _config = config;
        _inFlightMessages = inFlightMessages;
        Volatile.Write(ref _seqNum, lastSeqNo); // happens-before for Volatile.Read

        _processingResponseStream = RunProcessingWriteAck();
    }

    public async Task Write(ConcurrentQueue<MessageSending> toSendBuffer)
    {
        try
        {
            var writeMessage = new StreamWriteMessage.Types.WriteRequest
            {
                Codec = (int)_config.Codec
            };

            var currentSeqNum = Volatile.Read(ref _seqNum);

            while (toSendBuffer.TryDequeue(out var sendData))
            {
                if (sendData.Tcs.Task.IsFaulted)
                {
                    Logger.LogWarning("Message[SeqNo={SeqNo}] is cancelled", sendData.MessageData.SeqNo);

                    continue;
                }

                sendData.DisposedCtr.Unregister();

                var messageData = sendData.MessageData;

                if (messageData.SeqNo == default)
                {
                    messageData.SeqNo = ++currentSeqNum;
                }

                writeMessage.Messages.Add(messageData);
                _inFlightMessages.Enqueue(sendData);
            }

            Volatile.Write(ref _seqNum, currentSeqNum);
            await SendMessage(new MessageFromClient { WriteRequest = writeMessage });
        }
        catch (Driver.TransportException e)
        {
            Logger.LogError(e, "WriterSession[{SessionId}] have transport error on Write, last SeqNo={SeqNo}",
                SessionId, Volatile.Read(ref _seqNum));

            ReconnectSession();
        }
    }

    private async Task RunProcessingWriteAck()
    {
        try
        {
            Logger.LogInformation("WriterSession[{SessionId}] is running processing writeAck", SessionId);

            while (await Stream.MoveNextAsync())
            {
                var messageFromServer = Stream.Current;
                var status = Status.FromProto(messageFromServer.Status, messageFromServer.Issues);

                if (status.IsNotSuccess)
                {
                    Logger.LogError(
                        "WriterSession[{SessionId}] received unsuccessful status while processing writeAck: {Status}",
                        SessionId, status);
                    return;
                }

                foreach (var ack in messageFromServer.WriteResponse.Acks)
                {
                    if (!_inFlightMessages.TryPeek(out var messageFromClient))
                    {
                        Logger.LogCritical("No client message was found upon receipt of an acknowledgement: {WriteAck}",
                            ack);

                        break;
                    }

                    if (messageFromClient.MessageData.SeqNo > ack.SeqNo)
                    {
                        Logger.LogCritical(
                            @"The sequence number of the client's message in the queue is greater than the server's write acknowledgment number. 
Skipping the WriteAck... 
Client SeqNo: {SeqNo}, WriteAck: {WriteAck}",
                            messageFromClient.MessageData.SeqNo, ack);

                        continue;
                    }

                    if (messageFromClient.MessageData.SeqNo < ack.SeqNo)
                    {
                        Logger.LogCritical(
                            @"The sequence number of the client's message in the queue is less than the server's write acknowledgment number. 
Completing task on exception...
Client SeqNo: {SeqNo}, WriteAck: {WriteAck}",
                            messageFromClient.MessageData.SeqNo, ack);

                        messageFromClient.Tcs.TrySetException(new WriterException(
                            $"Client SeqNo[{messageFromClient.MessageData.SeqNo}] is less then server's WriteAck[{ack}]"));
                    }
                    else
                    {
                        messageFromClient.Tcs.TrySetResult(new WriteResult(ack));
                    }

                    _inFlightMessages.TryDequeue(out _); // Dequeue 
                }
            }

            Logger.LogInformation("WriterSession[{SessionId}]: stream is closed", SessionId);
        }
        catch (Driver.TransportException e)
        {
            Logger.LogError(e, "WriterSession[{SessionId}] have error on processing writeAck", SessionId);
        }
        catch (ObjectDisposedException)
        {
            Logger.LogDebug("WriterSession[{SessionId}]: stream is disposed", SessionId);
        }
        finally
        {
            ReconnectSession();
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
        Logger.LogDebug("WriterSession[{SessionId}]: start dispose process", SessionId);

        await Stream.RequestStreamComplete();
        await _processingResponseStream;

        Stream.Dispose();
    }
}
