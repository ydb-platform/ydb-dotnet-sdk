using System.Collections.Immutable;
using Ydb.Topic;

namespace Ydb.Sdk.Services.Topic.Reader;

public class Message<TValue>
{
    private readonly OffsetsRange _offsetsRange;
    private readonly ReaderSession<TValue> _readerSession;
    private readonly long _approximatelyBytesSize;

    internal Message(
        TValue data,
        string topic,
        long partitionId,
        string producerId,
        DateTime createdAt,
        ImmutableArray<Metadata> metadata,
        OffsetsRange offsetsRange,
        ReaderSession<TValue> readerSession,
        long approximatelyBytesSize)
    {
        Data = data;
        Topic = topic;
        PartitionId = partitionId;
        ProducerId = producerId;
        CreatedAt = createdAt;
        Metadata = metadata;

        _offsetsRange = offsetsRange;
        _readerSession = readerSession;
        _approximatelyBytesSize = approximatelyBytesSize;
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
        return _readerSession.CommitOffsetRange(_offsetsRange, PartitionId, _approximatelyBytesSize);
    }
}

public class BatchMessages<TValue>
{
    private readonly ReaderSession<TValue> _readerSession;
    private readonly OffsetsRange _offsetsRange;
    private readonly long _approximatelyBatchSize;

    public IReadOnlyCollection<Message<TValue>> Batch { get; }

    internal BatchMessages(
        IReadOnlyCollection<Message<TValue>> batch,
        ReaderSession<TValue> readerSession,
        long approximatelyBatchSize,
        OffsetsRange offsetsRange)
    {
        Batch = batch;
        _readerSession = readerSession;
        _approximatelyBatchSize = approximatelyBatchSize;
        _offsetsRange = offsetsRange;
    }

    public Task CommitBatchAsync()
    {
        return Batch.Count == 0
            ? Task.CompletedTask
            : _readerSession.CommitOffsetRange(_offsetsRange, Batch.First().PartitionId, _approximatelyBatchSize);
    }
}
