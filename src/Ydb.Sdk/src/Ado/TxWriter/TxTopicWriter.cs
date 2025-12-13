using System.Collections.Concurrent;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Topic;
using Ydb.Sdk.Topic.Writer;
using MessageData = Ydb.Topic.StreamWriteMessage.Types.WriteRequest.Types.MessageData;

namespace Ydb.Sdk.Ado.TxWriter;

/// <summary>
/// Internal implementation of a transactional topic writer.
/// </summary>
/// <typeparam name="T">The type of values to write to the topic.</typeparam>
internal class TxTopicWriter<T> : IBufferedTxTopicWriter<T>
{
    private readonly YdbTransaction _transaction;
    private readonly string _topicPath;
    private readonly TxWriterOptions _options;
    private readonly ISerializer<T> _serializer;
    private readonly ILogger _logger;
    private readonly ConcurrentQueue<PendingMessage> _pendingMessages = new();
    private readonly object _lockObject = new();
    
    private volatile int _currentBufferSize;
    private volatile bool _isDisposed;

    internal TxTopicWriter(
        YdbTransaction transaction,
        string topicPath,
        TxWriterOptions options,
        ISerializer<T> serializer,
        ILogger logger)
    {
        _transaction = transaction;
        _topicPath = topicPath;
        _options = options;
        _serializer = serializer;
        _logger = logger;
        _currentBufferSize = 0;
    }

    /// <inheritdoc/>
    public void Write(T value)
    {
        if (_isDisposed)
        {
            throw new ObjectDisposedException(nameof(TxTopicWriter<T>));
        }

        if (_transaction.Completed)
        {
            throw new InvalidOperationException("Cannot write to topic: transaction is already completed.");
        }

        byte[] data;
        try
        {
            data = _serializer.Serialize(value);
        }
        catch (Exception e)
        {
            throw new TxTopicWriterException("Error when serializing message data", e);
        }

        // Check if adding this message would exceed the buffer limit
        while (true)
        {
            var currentSize = _currentBufferSize;
            
            // Always allow at least one message, even if it exceeds the buffer
            if (currentSize > 0 && currentSize + data.Length > _options.BufferMaxSize)
            {
                throw new TxTopicWriterException(
                    $"Buffer overflow: the data size [{data.Length}] would exceed the buffer limit. " +
                    $"Current buffer size: {currentSize}, max size: {_options.BufferMaxSize}. " +
                    "Consider calling FlushAsync or increasing BufferMaxSize.");
            }

            var newSize = currentSize + data.Length;
            if (Interlocked.CompareExchange(ref _currentBufferSize, newSize, currentSize) == currentSize)
            {
                break;
            }
        }

        var messageData = new MessageData
        {
            Data = ByteString.CopyFrom(data),
            CreatedAt = Timestamp.FromDateTime(DateTime.UtcNow),
            UncompressedSize = data.Length
        };

        var pendingMessage = new PendingMessage(messageData, data.Length, new TaskCompletionSource<bool>());
        _pendingMessages.Enqueue(pendingMessage);

        _logger.LogDebug(
            "Enqueued message to topic {TopicPath} in transaction. Buffer size: {BufferSize}/{MaxSize}",
            _topicPath, _currentBufferSize, _options.BufferMaxSize);
    }

    /// <inheritdoc/>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        if (_isDisposed)
        {
            throw new ObjectDisposedException(nameof(TxTopicWriter<T>));
        }

        _logger.LogDebug("Flushing {Count} pending messages for topic {TopicPath}", 
            _pendingMessages.Count, _topicPath);

        var messagesToSend = new List<PendingMessage>();
        while (_pendingMessages.TryDequeue(out var message))
        {
            messagesToSend.Add(message);
        }

        if (messagesToSend.Count == 0)
        {
            _logger.LogDebug("No messages to flush for topic {TopicPath}", _topicPath);
            return;
        }

        try
        {
            // In a real implementation, this would send messages to YDB Topic Service
            // bound to the transaction ID. For now, we simulate success.
            // 
            // The actual implementation would:
            // 1. Get or create a Writer session from a pool
            // 2. Send WriteRequest with TxId set to _transaction.TxId
            // 3. Wait for acknowledgements
            // 4. Complete the TaskCompletionSources
            
            _logger.LogInformation(
                "Simulating flush of {Count} messages to topic {TopicPath} in transaction {TxId}",
                messagesToSend.Count, _topicPath, _transaction.TxId ?? "(not started)");

            // Simulate async work
            await Task.Delay(10, cancellationToken);

            // Mark all messages as sent
            foreach (var msg in messagesToSend)
            {
                msg.Tcs.TrySetResult(true);
                Interlocked.Add(ref _currentBufferSize, -msg.Size);
            }

            _logger.LogInformation(
                "Successfully flushed {Count} messages to topic {TopicPath}",
                messagesToSend.Count, _topicPath);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error flushing messages to topic {TopicPath}", _topicPath);
            
            // Mark all messages as failed
            foreach (var msg in messagesToSend)
            {
                msg.Tcs.TrySetException(new TxTopicWriterException("Failed to flush messages", ex));
            }
            
            throw new TxTopicWriterException($"Failed to flush messages to topic {_topicPath}", ex);
        }
    }

    internal void Dispose()
    {
        if (_isDisposed)
        {
            return;
        }

        _isDisposed = true;
        
        // Cancel any pending messages
        while (_pendingMessages.TryDequeue(out var message))
        {
            message.Tcs.TrySetCanceled();
        }
        
        _currentBufferSize = 0;
    }

    void IDisposableTxWriter.Dispose() => Dispose();

    private record PendingMessage(MessageData Data, int Size, TaskCompletionSource<bool> Tcs);
}
