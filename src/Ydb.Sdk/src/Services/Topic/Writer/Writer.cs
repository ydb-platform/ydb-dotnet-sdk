using System.Collections.Concurrent;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using Ydb.Topic;
using Ydb.Topic.V1;

namespace Ydb.Sdk.Services.Topic.Writer;

using InitResponse = StreamWriteMessage.Types.InitResponse;
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
    private readonly CancellationTokenSource _disposeTokenSource = new();

    private volatile TaskCompletionSource _taskWakeUpCompletionSource = new();
    private volatile IWriteSession _session = new NotStartedWriterSession("Session not started!");

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

    public Task<WriteResult> WriteAsync(TValue data)
    {
        return WriteAsync(new Message<TValue>(data));
    }

    public async Task<WriteResult> WriteAsync(Message<TValue> message)
    {
        TaskCompletionSource<WriteResult> completeTask = new();

        var data = _serializer.Serialize(message.Data);
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
                    _toSendBuffer.Enqueue(new MessageSending(messageData, completeTask));
                    WakeUpWorker();

                    break;
                }

                // Next try on race condition
                continue;
            }

            _logger.LogWarning(
                "Buffer overflow: the data size [{DataLength}] exceeds the current buffer limit ({CurLimitBufferSize}) [BufferMaxSize = {BufferMaxSize}]",
                data.Length, curLimitBufferSize, _config.BufferMaxSize);

            throw new WriterException("Buffer overflow");
        }

        try
        {
            var writeResult = await completeTask.Task;

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

        while (!_disposeTokenSource.Token.IsCancellationRequested)
        {
            await _taskWakeUpCompletionSource.Task;
            _taskWakeUpCompletionSource = new TaskCompletionSource();

            await _session.Write(_toSendBuffer);
        }
    }

    private void WakeUpWorker()
    {
        _taskWakeUpCompletionSource.TrySetResult();
    }

    private async Task Initialize()
    {
        try
        {
            if (_disposeTokenSource.IsCancellationRequested)
            {
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

            if (_config.MessageGroupId != null)
            {
                initRequest.MessageGroupId = _config.MessageGroupId;
            }

            _logger.LogDebug("Sending initialization request for the write stream: {InitRequest}", initRequest);

            await stream.Write(new MessageFromClient { InitRequest = initRequest });
            if (!await stream.MoveNextAsync())
            {
                _session = new NotStartedWriterSession(
                    $"Stream unexpectedly closed by YDB server. Current InitRequest: {initRequest}");

                _ = Task.Run(Initialize, _disposeTokenSource.Token);

                return;
            }

            var receivedInitMessage = stream.Current;

            var status = Status.FromProto(receivedInitMessage.Status, receivedInitMessage.Issues);

            if (status.IsNotSuccess)
            {
                _session = new NotStartedWriterSession("Initialization failed", status);

                if (status.StatusCode != StatusCode.SchemeError)
                {
                    _ = Task.Run(Initialize, _disposeTokenSource.Token);
                }

                _logger.LogCritical("Writer initialization failed to start. Reason: {Status}", status);

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

            var newSession = new WriterSession(
                _config,
                stream,
                initResponse,
                Initialize,
                e => { _session = new NotStartedWriterSession(e); },
                _logger,
                _inFlightMessages
            );
            
            if (!_inFlightMessages.IsEmpty)
            {
                var copyInFlightMessages = new ConcurrentQueue<MessageSending>();
                while (_inFlightMessages.TryDequeue(out var sendData))
                {
                    copyInFlightMessages.Enqueue(sendData);
                }

                await newSession.Write(copyInFlightMessages); // retry prev in flight messages
            }

            _session = newSession;
            newSession.RunProcessingWriteAck();
        }
        catch (Driver.TransportException e)
        {
            _logger.LogError(e, "Unable to connect the session");

            _session = new NotStartedWriterSession(
                new WriterException("Transport error on creating WriterSession", e));

            _ = Task.Run(Initialize, _disposeTokenSource.Token);
        }
    }

    public void Dispose()
    {
        try
        {
            _disposeTokenSource.Cancel();

            _session.Dispose();
        }
        finally
        {
            _disposeTokenSource.Dispose();
        }
    }
}

internal record MessageSending(MessageData MessageData, TaskCompletionSource<WriteResult> TaskCompletionSource);

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

    public NotStartedWriterSession(WriterException reasonException)
    {
        _reasonException = reasonException;
    }

    public Task Write(ConcurrentQueue<MessageSending> toSendBuffer)
    {
        while (toSendBuffer.TryDequeue(out var messageSending))
        {
            messageSending.TaskCompletionSource.TrySetException(_reasonException);
        }

        return Task.CompletedTask;
    }

    public void Dispose()
    {
    }
}

// No thread safe
internal class WriterSession : TopicSession<MessageFromClient, MessageFromServer>, IWriteSession
{
    private readonly WriterConfig _config;
    private readonly ConcurrentQueue<MessageSending> _inFlightMessages;

    private long _seqNum;

    public WriterSession(
        WriterConfig config,
        WriterStream stream,
        InitResponse initResponse,
        Func<Task> initialize,
        Action<WriterException> resetSessionOnTransportError,
        ILogger logger,
        ConcurrentQueue<MessageSending> inFlightMessages
    ) : base(
        stream,
        logger,
        initResponse.SessionId,
        initialize,
        resetSessionOnTransportError
    )
    {
        _config = config;
        _inFlightMessages = inFlightMessages;
        Volatile.Write(ref _seqNum, initResponse.LastSeqNo); // happens-before for Volatile.Read
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

                messageData.SeqNo = ++currentSeqNum;
                writeMessage.Messages.Add(messageData);
                _inFlightMessages.Enqueue(sendData);
            }

            Volatile.Write(ref _seqNum, currentSeqNum);
            await Stream.Write(new MessageFromClient { WriteRequest = writeMessage });
        }
        catch (Driver.TransportException e)
        {
            Logger.LogError(e, "WriterSession[{SessionId}] have error on Write, last SeqNo={SeqNo}",
                SessionId, Volatile.Read(ref _seqNum));

            ReconnectSession(new WriterException("Transport error in the WriterSession on write messages", e));
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
                    Logger.LogWarning(
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

                        messageFromClient.TaskCompletionSource.TrySetException(new WriterException(
                            $"Client SeqNo[{messageFromClient.MessageData.SeqNo}] is less then server's WriteAck[{ack}]"));
                    }
                    else
                    {
                        messageFromClient.TaskCompletionSource.TrySetResult(new WriteResult(ack));
                    }

                    _inFlightMessages.TryDequeue(out _); // Dequeue 
                }
            }
        }
        catch (Driver.TransportException e)
        {
            Logger.LogError(e, "WriterSession[{SessionId}] have error on processing writeAck", SessionId);

            ReconnectSession(new WriterException("Transport error in the WriterSession on processing writeAck", e));

            return;
        }

        Logger.LogWarning("WriterSession[{SessionId}]: stream is closed", SessionId);

        ReconnectSession(new WriterException("WriterStream is closed"));
    }
}
