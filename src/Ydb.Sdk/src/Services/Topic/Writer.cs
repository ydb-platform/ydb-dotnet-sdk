using System.Collections.Concurrent;
using System.Transactions;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using Ydb.Topic;
using Ydb.Topic.V1;

namespace Ydb.Sdk.Services.Topic;

using InitResponse = StreamWriteMessage.Types.InitResponse;
using MessageData = StreamWriteMessage.Types.WriteRequest.Types.MessageData;
using MessageFromClient = StreamWriteMessage.Types.FromClient;
using MessageFromServer = StreamWriteMessage.Types.FromServer;
using WriterStream = Driver.BidirectionalStream<
    StreamWriteMessage.Types.FromClient,
    StreamWriteMessage.Types.FromServer
>;

internal class Writer<TValue> : IWriter<TValue>
{
    private readonly Driver _driver;
    private readonly WriterConfig _config;
    private readonly ILogger<Writer<TValue>> _logger;
    private readonly ISerializer<TValue> _serializer;

    private readonly ConcurrentQueue<MessageSending> _inFlightMessages = new();
    private readonly ConcurrentQueue<MessageSending> _toSendBuffer = new();
    private readonly SemaphoreSlim _writeSemaphoreSlim = new(1);

    private volatile WriterSession _session = null!;

    internal Writer(Driver driver, WriterConfig config, ISerializer<TValue> serializer)
    {
        _driver = driver;
        _config = config;
        _serializer = serializer;
        _logger = driver.LoggerFactory.CreateLogger<Writer<TValue>>();
    }

    internal async Task Initialize()
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
            throw new YdbWriterException(
                $"Stream unexpectedly closed by YDB server. Current InitRequest: {initRequest}");
        }

        var receivedInitMessage = stream.Current;

        Status.FromProto(receivedInitMessage.Status, receivedInitMessage.Issues).EnsureSuccess();

        var initResponse = receivedInitMessage.InitResponse;

        _logger.LogDebug("Received a response for the initialization request on the write stream: {InitResponse}",
            initResponse);

        if (!initResponse.SupportedCodecs.Codecs.Contains((int)_config.Codec))
        {
            throw new YdbWriterException($"Topic[{_config.TopicPath}] is not supported codec: {_config.Codec}");
        }

        _session = new WriterSession(_config, stream, initResponse, Initialize, _logger);

        await _writeSemaphoreSlim.WaitAsync();
        try
        {
            _logger.LogDebug("Retrying to send pending in-flight messages after stream restart");

            await _session.Write(_inFlightMessages, _inFlightMessages);
        }
        finally
        {
            _writeSemaphoreSlim.Release();
        }

        _ = _session.RunProcessingWriteAck(_inFlightMessages);
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

        _toSendBuffer.Enqueue(new MessageSending(messageData, completeTask));

        if (_toSendBuffer.IsEmpty) // concurrent sending
        {
            return await completeTask.Task;
        }

        await _writeSemaphoreSlim.WaitAsync();
        try
        {
            await _session.Write(_toSendBuffer, _inFlightMessages);
        }
        finally
        {
            _writeSemaphoreSlim.Release();
        }

        return await completeTask.Task;
    }
}

// No thread safe
internal class WriterSession : TopicSession<MessageFromClient, MessageFromServer>
{
    private readonly WriterConfig _config;

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
    }

    internal async Task RunProcessingWriteAck(ConcurrentQueue<MessageSending> inFlightMessages)
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
                    if (!inFlightMessages.TryPeek(out var messageFromClient))
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

                    inFlightMessages.TryDequeue(out _); // Dequeue 
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

    internal async Task Write(ConcurrentQueue<MessageSending> toSendBuffer,
        ConcurrentQueue<MessageSending> inFlightMessages)
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
                inFlightMessages.Enqueue(sendData);
            }

            Volatile.Write(ref _seqNum, currentSeqNum);
            await Stream.Write(new MessageFromClient { WriteRequest = writeMessage });
        }
        catch (TransactionException e)
        {
            Logger.LogError(e, "WriterSession[{SessionId}] have error on Write, last SeqNo={SeqNo}",
                SessionId, Volatile.Read(ref _seqNum));

            ReconnectSession();
        }
    }
}

internal record MessageSending(MessageData MessageData, TaskCompletionSource<WriteResult> TaskCompletionSource);
