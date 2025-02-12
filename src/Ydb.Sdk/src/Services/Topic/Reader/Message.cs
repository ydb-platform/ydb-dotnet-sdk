using System.Collections.Immutable;
using Ydb.Topic;

namespace Ydb.Sdk.Services.Topic.Reader;

public class Message<TValue>
{
    private readonly long _partitionSessionId;
    private readonly OffsetsRange _offsetsRange;
    private readonly ReaderSession<TValue> _readerSession;

    internal Message(
        TValue data,
        string topic,
        long partitionId,
        long partitionSessionId,
        string producerId,
        DateTime createdAt,
        ImmutableArray<Metadata> metadata,
        OffsetsRange offsetsRange,
        ReaderSession<TValue> readerSession)
    {
        Data = data;
        Topic = topic;
        PartitionId = partitionId;
        ProducerId = producerId;
        CreatedAt = createdAt;
        Metadata = metadata;

        _partitionSessionId = partitionSessionId;
        _offsetsRange = offsetsRange;
        _readerSession = readerSession;
    }

    public TValue Data { get; }

    /// <summary>
    /// The topic associated with the message.
    /// </summary>
    public string Topic { get; }

    public long PartitionId { get; }

    public string ProducerId { get; }

    public DateTime CreatedAt { get; }

    public IReadOnlyCollection<Metadata> Metadata { get; }

    public Task CommitAsync()
    {
        return _readerSession.CommitOffsetRange(_offsetsRange, _partitionSessionId);
    }
}

public class BatchMessages<TValue>
{
    private readonly ReaderSession<TValue> _readerSession;
    private readonly OffsetsRange _offsetsRange;
    private readonly long _partitionSessionId;

    public IReadOnlyList<Message<TValue>> Batch { get; }

    internal BatchMessages(
        IReadOnlyList<Message<TValue>> batch,
        ReaderSession<TValue> readerSession,
        OffsetsRange offsetsRange,
        long partitionSessionId)
    {
        Batch = batch;
        _readerSession = readerSession;
        _offsetsRange = offsetsRange;
        _partitionSessionId = partitionSessionId;
    }

    public Task CommitBatchAsync()
    {
        return Batch.Count == 0
            ? Task.CompletedTask
            : _readerSession.CommitOffsetRange(_offsetsRange, _partitionSessionId);
    }
}
