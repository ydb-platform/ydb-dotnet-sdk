using System.Collections.Concurrent;
using System.Transactions;
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
using WriterStream = BidirectionalStream<
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

    private volatile TaskCompletionSource _taskWakeUpCompletionSource = new();
    private volatile IWriteSession _session = null!;
    private volatile bool _disposed;

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
            CreatedAt = Timestamp.FromDateTime(message.Timestamp),
            UncompressedSize = data.Length
        };

        foreach (var metadata in message.Metadata)
        {
            messageData.MetadataItems.Add(
                new MetadataItem { Key = metadata.Key, Value = ByteString.CopyFrom(metadata.Value) }
            );
        }

        while (true)
        {
            var curLimitBufferSize = Volatile.Read(ref _limitBufferMaxSize);

            if ( // sending one biggest message anyway
                curLimitBufferSize == _config.BufferMaxSize && data.Length > curLimitBufferSize
                || curLimitBufferSize >= data.Length)
            {
                if (Interlocked.CompareExchange(ref _limitBufferMaxSize, curLimitBufferSize,
                        curLimitBufferSize - data.Length) == curLimitBufferSize)
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

            throw new YdbWriterException("Buffer overflow");
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

        while (!_disposed)
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

            _ = Task.Run(Initialize);

            return;
        }

        var receivedInitMessage = stream.Current;

        var status = Status.FromProto(receivedInitMessage.Status, receivedInitMessage.Issues);

        if (status.IsNotSuccess)
        {
            _session = new NotStartedWriterSession(status.ToString());

            _ = Task.Run(Initialize);

            return;
        }

        var initResponse = receivedInitMessage.InitResponse;

        _logger.LogDebug("Received a response for the initialization request on the write stream: {InitResponse}",
            initResponse);

        if (!initResponse.SupportedCodecs.Codecs.Contains((int)_config.Codec))
        {
            _logger.LogCritical("Topic[{TopicPath}] is not supported codec: {Codec}", _config.TopicPath, _config.Codec);

            _session = new NotStartedWriterSession(
                $"Topic[{_config.TopicPath}] is not supported codec: {_config.Codec}");
            return;
        }

        _session = new WriterSession(_config, stream, initResponse, Initialize, _logger);
    }

    public void Dispose()
    {
        _disposed = true;
    }
}

internal record MessageSending(MessageData MessageData, TaskCompletionSource<WriteResult> TaskCompletionSource);

internal interface IWriteSession
{
    Task Write(ConcurrentQueue<MessageSending> toSendBuffer);
}

internal class NotStartedWriterSession : IWriteSession
{
    private readonly YdbWriterException _reasonException;

    public NotStartedWriterSession(string reasonExceptionMessage)
    {
        _reasonException = new YdbWriterException(reasonExceptionMessage);
    }

    public Task Write(ConcurrentQueue<MessageSending> toSendBuffer)
    {
        foreach (var messageSending in toSendBuffer)
        {
            messageSending.TaskCompletionSource.SetException(_reasonException);
        }

        return Task.CompletedTask;
    }
}

// No thread safe
internal class WriterSession : TopicSession<MessageFromClient, MessageFromServer>, IWriteSession
{
    private readonly WriterConfig _config;
    private readonly ConcurrentQueue<MessageSending> _inFlightMessages = new();

    private long _seqNum;

    public WriterSession(
        WriterConfig config,
        WriterStream stream,
        InitResponse initResponse,
        Func<Task> initialize,
        ILogger logger) : base(stream, logger, initResponse.SessionId, initialize)
    {
        _config = config;
        Volatile.Write(ref _seqNum, initResponse.LastSeqNo); // happens-before for Volatile.Read

        RunProcessingWriteAck();
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
        catch (TransactionException e)
        {
            Logger.LogError(e, "WriterSession[{SessionId}] have error on Write, last SeqNo={SeqNo}",
                SessionId, Volatile.Read(ref _seqNum));

            ReconnectSession();

            while (_inFlightMessages.TryDequeue(out var sendData))
            {
                sendData.TaskCompletionSource.SetException(e);
            }
        }
    }

    private async void RunProcessingWriteAck()
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

                        messageFromClient.TaskCompletionSource.SetException(new YdbWriterException(
                            $"Client SeqNo[{messageFromClient.MessageData.SeqNo}] is less then server's WriteAck[{ack}]"));
                    }
                    else
                    {
                        messageFromClient.TaskCompletionSource.SetResult(new WriteResult(ack));
                    }

                    _inFlightMessages.TryDequeue(out _); // Dequeue 
                }
            }
        }
        catch (Exception e)
        {
            Logger.LogError(e, "WriterSession[{SessionId}] have error on processing writeAck", SessionId);
        }
        finally
        {
            ReconnectSession();
        }
    }
}
