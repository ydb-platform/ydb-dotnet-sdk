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
using ProducerStream = Driver.BidirectionalStream<
    StreamWriteMessage.Types.FromClient,
    StreamWriteMessage.Types.FromServer
>;

internal class Producer<TValue> : IProducer<TValue>
{
    private readonly ProducerConfig _config;
    private readonly ILogger<Producer<TValue>> _logger;
    private readonly ISerializer<TValue> _serializer;

    private readonly ConcurrentQueue<MessageSending> _inFlightMessages = new();
    private readonly ConcurrentQueue<MessageSending> _toSendBuffer = new();
    private readonly SemaphoreSlim _writeSemaphoreSlim = new(1);

    private volatile ProducerSession _session = null!;

    internal Producer(ProducerConfig producerConfig, ISerializer<TValue> serializer)
    {
        _config = producerConfig;
        _serializer = serializer;
        _logger = producerConfig.Driver.LoggerFactory.CreateLogger<Producer<TValue>>();
    }

    internal async Task Initialize()
    {
        _logger.LogInformation("Producer session initialization started. ProducerConfig: {ProducerConfig}", _config);

        var stream = _config.Driver.BidirectionalStreamCall(
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

        await stream.Write(new MessageFromClient { InitRequest = initRequest });
        if (!await stream.MoveNextAsync())
        {
            throw new YdbProducerException(
                $"Stream unexpectedly closed by YDB server. Current InitRequest: {initRequest}");
        }

        var receivedInitMessage = stream.Current;

        Status.FromProto(receivedInitMessage.Status, receivedInitMessage.Issues).EnsureSuccess();

        var initResponse = receivedInitMessage.InitResponse;

        if (!initResponse.SupportedCodecs.Codecs.Contains((int)_config.Codec))
        {
            throw new YdbProducerException($"Topic is not supported codec: {_config.Codec}");
        }

        _session = new ProducerSession(_config, stream, initResponse, Initialize, _logger);
        _ = _session.RunProcessingWriteAck(_inFlightMessages);
    }

    public Task<SendResult> SendAsync(TValue data)
    {
        return SendAsync(new Message<TValue>(data));
    }

    public async Task<SendResult> SendAsync(Message<TValue> message)
    {
        TaskCompletionSource<SendResult> completeTask = new();

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
internal class ProducerSession : TopicSession
{
    private readonly ProducerConfig _config;
    private readonly ProducerStream _stream;

    private long _seqNum;

    public ProducerSession(
        ProducerConfig config,
        ProducerStream stream,
        InitResponse initResponse,
        Func<Task> initialize,
        ILogger logger) : base(logger, initResponse.SessionId, initialize)
    {
        _config = config;
        _stream = stream;
        _seqNum = initResponse.LastSeqNo;
    }

    internal async Task RunProcessingWriteAck(ConcurrentQueue<MessageSending> inFlightMessages)
    {
        try
        {
            Logger.LogInformation("ProducerSession[{SessionId}] is running processing writeAck", SessionId);

            await foreach (var messageFromServer in _stream)
            {
                var status = Status.FromProto(messageFromServer.Status, messageFromServer.Issues);

                if (status.IsNotSuccess)
                {
                    Logger.LogWarning(
                        "ProducerSession[{SessionId}] received unsuccessful status while processing writeAck: {Status}",
                        SessionId, status);
                    return;
                }

                foreach (var ack in messageFromServer.WriteResponse.Acks)
                {
                    if (!inFlightMessages.TryDequeue(out var messageFromClient))
                    {
                        break;
                    }

                    messageFromClient.TaskCompletionSource.SetResult(new SendResult(ack));
                }
            }
        }
        catch (Exception e)
        {
            Logger.LogError(e, "ProducerSession[{SessionId}] have error on processing writeAck", SessionId);
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
            await _stream.Write(new MessageFromClient { WriteRequest = writeMessage });
        }
        catch (TransactionException e)
        {
            ReconnectSession();

            Console.WriteLine(e);
            throw;
        }
    }

    public ValueTask DisposeAsync()
    {
        return _stream.DisposeAsync();
    }
}

internal record MessageSending(MessageData MessageData, TaskCompletionSource<SendResult> TaskCompletionSource);
