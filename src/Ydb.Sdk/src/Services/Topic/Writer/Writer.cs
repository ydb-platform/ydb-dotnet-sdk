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
    private readonly ConcurrentQueue<MessageSending> _toSendBuffer = new();
    private readonly ConcurrentQueue<MessageSending> _inFlightMessages = new();
    private readonly CancellationTokenSource _disposeCts = new();

    private volatile TaskCompletionSource _tcsWakeUp = new();
    private volatile IWriteSession _session = null!;

    private int _limitBufferMaxSize;

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
        cancellationToken.Register(
            () => tcs.TrySetException(
                new WriterException("The write operation was canceled before it could be completed")
            ), useSynchronizationContext: false
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
            var curLimitBufferSize = Volatile.Read(ref _limitBufferMaxSize);

            if ( // sending one biggest message anyway
                (curLimitBufferSize == _config.BufferMaxSize && data.Length > curLimitBufferSize)
                || curLimitBufferSize >= data.Length)
            {
                if (Interlocked.CompareExchange(ref _limitBufferMaxSize,
                        curLimitBufferSize - data.Length, curLimitBufferSize) == curLimitBufferSize)
                {
                    _toSendBuffer.Enqueue(new MessageSending(messageData, tcs));
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
                await Task.Delay(_config.BufferOverflowRetryTimeoutMs, cancellationToken);
            }
            catch (TaskCanceledException)
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
        }
    }

    private async void StartWriteWorker()
    {
        await Initialize();

        while (!_disposeCts.Token.IsCancellationRequested)
        {
            await _tcsWakeUp.Task;
            _tcsWakeUp = new TaskCompletionSource();

            if (_toSendBuffer.IsEmpty)
            {
                continue;
            }

            await _session.Write(_toSendBuffer);
        }
    }

    private void WakeUpWorker()
    {
        _tcsWakeUp.TrySetResult();
    }

    private async Task Initialize()
    {
        _session = DummyWriteSession.Instance;

        try
        {
            if (_disposeCts.IsCancellationRequested)
            {
                _logger.LogWarning("Initialize writer is canceled because it has been disposed");

                return;
            }

            _logger.LogInformation("Writer session initialization started. WriterConfig: {WriterConfig}", _config);

            var stream = _driver.BidirectionalStreamCall(
                TopicService.StreamWriteMethod,
                GrpcRequestSettings.DefaultInstance
            );

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

                _ = Task.Run(Initialize, _disposeCts.Token);

                return;
            }

            var receivedInitMessage = stream.Current;

            var status = Status.FromProto(receivedInitMessage.Status, receivedInitMessage.Issues);

            if (status.IsNotSuccess)
            {
                if (RetrySettings.DefaultInstance.GetRetryRule(status.StatusCode).Policy != RetryPolicy.None)
                {
                    _logger.LogError("Writer initialization failed to start. Reason: {Status}", status);

                    _ = Task.Run(Initialize, _disposeCts.Token);
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

            var copyInFlightMessages = new ConcurrentQueue<MessageSending>();
            var lastSeqNo = initResponse.LastSeqNo;
            while (_inFlightMessages.TryDequeue(out var sendData))
            {
                if (sendData.Tcs.Task.IsFaulted)
                {
                    _logger.LogWarning("Message[SeqNo={SeqNo}] is cancelled", sendData.MessageData.SeqNo);

                    continue;
                }

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
                logger: _logger,
                inFlightMessages: _inFlightMessages
            );

            if (!copyInFlightMessages.IsEmpty)
            {
                await newSession.Write(copyInFlightMessages); // retry prev in flight messages    
            }

            _session = newSession;
            newSession.RunProcessingWriteAck();
            WakeUpWorker(); // attempt send buffer        
        }
        catch (Driver.TransportException e)
        {
            _logger.LogError(e, "Transport error on creating WriterSession");

            _ = Task.Run(Initialize, _disposeCts.Token);
        }
    }

    public void Dispose()
    {
        try
        {
            _disposeCts.Cancel();

            _session.Dispose();
        }
        finally
        {
            _disposeCts.Dispose();
        }
    }
}

internal record MessageSending(MessageData MessageData, TaskCompletionSource<WriteResult> Tcs);

internal interface IWriteSession : IDisposable
{
    Task Write(ConcurrentQueue<MessageSending> toSendBuffer);
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

    public void Dispose()
    {
        // Do nothing 
    }
}

internal class DummyWriteSession : IWriteSession
{
    internal static readonly DummyWriteSession Instance = new();

    private DummyWriteSession()
    {
    }

    public void Dispose()
    {
    }

    public Task Write(ConcurrentQueue<MessageSending> toSendBuffer)
    {
        return Task.CompletedTask;
    }
}

internal class WriterSession : TopicSession<MessageFromClient, MessageFromServer>, IWriteSession
{
    private readonly WriterConfig _config;
    private readonly ConcurrentQueue<MessageSending> _inFlightMessages;

    private long _seqNum;

    public WriterSession(
        WriterConfig config,
        WriterStream stream,
        long lastSeqNo,
        string sessionId,
        Func<Task> initialize,
        ILogger logger,
        ConcurrentQueue<MessageSending> inFlightMessages
    ) : base(
        stream,
        logger,
        sessionId,
        initialize
    )
    {
        _config = config;
        _inFlightMessages = inFlightMessages;
        Volatile.Write(ref _seqNum, lastSeqNo); // happens-before for Volatile.Read
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
                var messageData = sendData.MessageData;

                if (messageData.SeqNo == default)
                {
                    messageData.SeqNo = ++currentSeqNum;
                }

                writeMessage.Messages.Add(messageData);
                _inFlightMessages.Enqueue(sendData);
            }

            Volatile.Write(ref _seqNum, currentSeqNum);
            await Stream.Write(new MessageFromClient { WriteRequest = writeMessage });
        }
        catch (Driver.TransportException e)
        {
            Logger.LogError(e, "WriterSession[{SessionId}] have transport error on Write, last SeqNo={SeqNo}",
                SessionId, Volatile.Read(ref _seqNum));

            ReconnectSession();
        }
    }

    internal async void RunProcessingWriteAck()
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
        }
        catch (Driver.TransportException e)
        {
            Logger.LogError(e, "WriterSession[{SessionId}] have error on processing writeAck", SessionId);

            ReconnectSession();

            return;
        }

        Logger.LogWarning("WriterSession[{SessionId}]: stream is closed", SessionId);

        ReconnectSession();
    }
}
