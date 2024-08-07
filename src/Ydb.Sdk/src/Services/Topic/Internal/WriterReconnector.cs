using Microsoft.Extensions.Logging;
using Ydb.Sdk.GrpcWrappers.Topic.Exceptions;
using Ydb.Sdk.GrpcWrappers.Topic.Writer.Init;
using Ydb.Sdk.GrpcWrappers.Topic.Writer.Write;
using Ydb.Sdk.Services.Topic.Models.Writer;
using Ydb.Sdk.Utils;
using Codec = Ydb.Sdk.GrpcWrappers.Topic.Codecs.Codec;
using StreamWriter = Ydb.Sdk.GrpcWrappers.Topic.StreamWriter;
using PublicMessage = Ydb.Sdk.Services.Topic.Models.Writer.Message;
using PublicCodec = Ydb.Sdk.Services.Topic.Models.Codec;
using Message = Ydb.Sdk.GrpcWrappers.Topic.Writer.Write.Message;

namespace Ydb.Sdk.Services.Topic.Internal;

internal class WriterReconnector
{
    private readonly List<Task> _backgroundTasks;
    private readonly CancellationTokenSource _backgroundTasksCancellationSource = new();
    private readonly PublicCodec _codec;
    private readonly WriterConfig _config;
    private readonly Driver _driver;
    private readonly Encoders _encoders = new();
    private readonly InitRequest _initRequest;
    private readonly ILogger<WriterReconnector> _logger;
    private readonly Queue<Message> _messagesToEncodeQueue = new();
    private readonly Queue<Message> _messagesToSendQueue = new();
    private readonly RetrySettings _retrySettings = new();
    private readonly TaskCompletionSource _stopReason;
    private readonly Queue<TaskCompletionSource<WriteResult>> _writeResults = new();

    private bool _closed;
    private InitInfo? _initInfo;
    private long _lastKnownSequenceNumber;

    public WriterReconnector(Driver driver, WriterConfig config)
    {
        _logger = driver.LoggerFactory.CreateLogger<WriterReconnector>();
        _codec = config.Codec ?? PublicCodec.Gzip;
        if (config.Encoders != null)
        {
            foreach (var encoder in config.Encoders)
            {
                var internalCodec = EnumConverter.Convert<PublicCodec, Codec>(encoder.Key);
                _encoders.Add(internalCodec, encoder.Value);
            }
        }

        if (!_encoders.HasEncoder(EnumConverter.Convert<PublicCodec, Codec>(_codec)))
            throw new Exception($"No encoder for codec {_codec}");

        _driver = driver;
        _initRequest = config.ToInitRequest();
        _config = config;
        _stopReason = new TaskCompletionSource();
        var cancelBackgroundToken = _backgroundTasksCancellationSource.Token;
        _backgroundTasks = new List<Task>
        {
            Task.Run(async () => await ConnectionLoop(cancelBackgroundToken)),
            Task.Run(async () => await EncodeLoop(cancelBackgroundToken))
        };
    }

    public async Task<List<Task<WriteResult>>> Write(List<PublicMessage> messages)
    {
        EnsureNotStopped();
        if (_config.AutoSetSequenceNumber)
            await WaitInit();

        var internalMessages = PrepareMessages(messages);
        var taskSources = Enumerable
            .Range(0, messages.Count)
            .Select(_ => new TaskCompletionSource<WriteResult>())
            .ToList();
        taskSources.ForEach(source => _writeResults.Enqueue(source));

        if (_codec == PublicCodec.Raw)
        {
            EnqueueSendMessages(internalMessages);
        }
        else
        {
            foreach (var message in internalMessages)
            {
                _messagesToEncodeQueue.Enqueue(message);
            }
        }

        return taskSources.Select(s => s.Task).ToList();
    }

    public async Task<InitInfo> WaitInit()
    {
        return await Task.Run(() =>
        {
            while (true)
            {
                EnsureNotStopped();

                if (_initInfo != null)
                    return _initInfo;
            }
        });
    }

    public async Task Close(bool needFlush)
    {
        if (_closed)
            return;
        _closed = true;
        _logger.LogDebug("Closing writer reconnector");

        if (needFlush)
            await Flush();

        _backgroundTasksCancellationSource.Cancel();
        await Task.WhenAll(_backgroundTasks);

        try
        {
            EnsureNotStopped();
        }
        catch (TopicWriterStoppedException)
        {
        }
    }

    public async Task Flush()
    {
        await Task.WhenAll(_writeResults.Select(source => source.Task));
    }

    private void EnsureNotStopped()
    {
        if (_stopReason.Task is {IsCompleted: true, Exception: not null})
            throw _stopReason.Task.Exception;
    }

    private async Task ConnectionLoop(CancellationToken cancellationToken)
    {
        //TODO use SessionPool?
        while (!cancellationToken.IsCancellationRequested)
        {
            var attempt = 0u;
            var tasks = new List<Task>();
            StreamWriter? streamWriter = null;
            try
            {
                streamWriter = await StreamWriter.Init(_driver, _initRequest);

                if (_initInfo == null)
                {
                    _lastKnownSequenceNumber = streamWriter.LastSequenceNumber;
                    _initInfo = new InitInfo
                    {
                        LastSequenceNumber = streamWriter.LastSequenceNumber,
                        SupportedCodecs = streamWriter.SupportedCodecs.ToPublic().ToList()
                    };
                }

                var sendLoopTask = Task.Run(async () => await SendLoop(streamWriter, cancellationToken),
                    cancellationToken);
                var receiveLoopTask = Task.Run(async () => await ReadLoop(streamWriter, cancellationToken),
                    cancellationToken);
                tasks.Add(sendLoopTask);
                tasks.Add(receiveLoopTask);
                await Task.WhenAny(tasks);
            }
            catch (StatusUnsuccessfulException e)
            {
                attempt++;
                var retryRule = _retrySettings.GetRetryRule(e.Status.StatusCode);
                var isRetriable = (retryRule.Idempotency == Idempotency.Idempotent && _retrySettings.IsIdempotent) ||
                                  retryRule.Idempotency == Idempotency.NonIdempotent;
                if (!isRetriable || attempt > _retrySettings.MaxAttempts)
                {
                    StopWriter(e);
                    return;
                }

                await Task.Delay(retryRule.BackoffSettings.CalcBackoff(attempt), cancellationToken);
            }
            catch (Exception e)
            {
                StopWriter(e);
            }
            finally
            {
                _backgroundTasksCancellationSource.Cancel();
                await Task.WhenAll(tasks);
                if (streamWriter != null)
                    await streamWriter.Close();
            }
        }
    }

    private async Task SendLoop(StreamWriter streamWriter, CancellationToken cts)
    {
        var codec = EnumConverter.Convert<PublicCodec, Codec>(_codec);
        try
        {
            while (!cts.IsCancellationRequested)
            {
                if (!_messagesToSendQueue.Any())
                    continue;
                var message = _messagesToSendQueue.Dequeue();
                await streamWriter.Write(new WriteRequest
                {
                    Codec = codec,
                    Messages = new List<Message> {message}
                });
            }
        }
        catch (Exception e)
        {
            StopWriter(e);
            throw;
        }
    }

    private async Task ReadLoop(StreamWriter streamWriter, CancellationToken cts)
    {
        while (!cts.IsCancellationRequested)
        {
            var response = await streamWriter.Receive();
            response.Result.Acks.ForEach(HandleAck);
        }
    }

    private async Task EncodeLoop(CancellationToken cancellationToken)
    {
        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var messages = new List<Message>();
                while (_messagesToEncodeQueue.Any())
                    messages.Add(_messagesToEncodeQueue.Dequeue());
                await EncodeMessages(_codec, messages);
                foreach (var message in messages)
                {
                    _messagesToSendQueue.Enqueue(message);
                }
            }
        }
        catch (Exception e)
        {
            StopWriter(e);
        }
    }

    private void HandleAck(WriteAck ack)
    {
        var taskSource = _writeResults.Dequeue();
        var writeStatus = ack.WriteStatus;
        switch (writeStatus.Type)
        {
            case WriteStatusType.Skipped:
            case WriteStatusType.Written:
                taskSource.SetResult(WriteResult.FromWrapper(writeStatus));
                break;
            default:
                throw new Exception($"Received ack with unexpected status {writeStatus.Type}");
        }
    }

    private void EnqueueSendMessages(IEnumerable<Message> messages)
    {
        foreach (var message in messages)
        {
            _messagesToSendQueue.Enqueue(message);
        }
    }

    private async Task EncodeMessages(PublicCodec publicCodec, List<Message> messages)
    {
        if (publicCodec == PublicCodec.Raw)
            return;

        var internalCodec = EnumConverter.Convert<PublicCodec, Codec>(publicCodec);
        var tasks = messages.Select(m => Task.Run(() => _encoders.Encode(internalCodec, m.Data)));
        var encodedContents = await Task.WhenAll(tasks);
        for (var i = 0; i < encodedContents.Length; i++)
        {
            messages[i].Codec = internalCodec;
            messages[i].Data = encodedContents[i];
        }
    }

    private List<Message> PrepareMessages(List<PublicMessage> publicMessages)
    {
        var now = _config.AutoSetCreatedAt ? DateTime.UtcNow : default;

        var result = new List<Message>();
        foreach (var internalMessage in publicMessages.Select(message => message.ToWrapper()))
        {
            if (_config.AutoSetSequenceNumber)
            {
                if (internalMessage.SequenceNumber >= 0)
                {
                    throw new InvalidOperationException(
                        "Sequence number shouldn't be set when using it's auto increment");
                }

                _lastKnownSequenceNumber++;
                internalMessage.SequenceNumber = _lastKnownSequenceNumber;
            }
            else
            {
                if (internalMessage.SequenceNumber < 0)
                {
                    throw new InvalidOperationException(
                        "Message sequence number must be set in case of using it's manual controlling");
                }

                if (internalMessage.SequenceNumber <= _lastKnownSequenceNumber)
                {
                    throw new InvalidOperationException(
                        "Duplicated message. Sequence number must be more then current max value");
                }

                _lastKnownSequenceNumber = internalMessage.SequenceNumber;
            }

            if (_config.AutoSetCreatedAt)
            {
                if (internalMessage.CreatedAt != default)
                {
                    throw new InvalidOperationException(
                        "Message creation time shouldn't be set when using it's auto increment");
                }

                internalMessage.CreatedAt = now;
            }

            result.Add(internalMessage);
        }

        return result;
    }

    private void StopWriter(Exception reason)
    {
        if (_stopReason.Task.IsCompleted)
            return;

        _stopReason.SetException(reason);
        foreach (var result in _writeResults)
        {
            result.SetException(reason);
        }

        _logger.LogInformation("Stopped writer because of {reason}", reason);
    }
}
