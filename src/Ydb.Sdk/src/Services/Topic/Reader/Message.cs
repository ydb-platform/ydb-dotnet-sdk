using System.Collections.Immutable;
using Ydb.Topic;

namespace Ydb.Sdk.Services.Topic.Reader;

public class Message<TValue>
{
    private readonly OffsetsRange _offsetsRange;
    private readonly ReaderSession _readerSession;

    internal Message(
        TValue data,
        string topic,
        long partitionId,
        string producerId,
        DateTime createdAt,
        ImmutableArray<Metadata> metadata,
        OffsetsRange offsetsRange,
        ReaderSession readerSession)
    {
        Data = data;
        Topic = topic;
        PartitionId = partitionId;
        ProducerId = producerId;
        CreatedAt = createdAt;
        Metadata = metadata;

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

    public ImmutableArray<Metadata> Metadata { get; }

    internal long Start => _offsetsRange.Start;
    internal long End => _offsetsRange.End;

    public Task CommitAsync()
    {
        return _readerSession.CommitOffsetRange(_offsetsRange, PartitionId);
    }
}

public class BatchMessage<TValue>
{
    private readonly ReaderSession _readerSession;

    public ImmutableArray<Message<TValue>> Batch { get; }

    internal BatchMessage(
        ImmutableArray<Message<TValue>> batch,
        ReaderSession readerSession)
    {
        Batch = batch;
        _readerSession = readerSession;
    }

    public Task CommitBatchAsync()
    {
        if (Batch.Length == 0)
        {
            return Task.CompletedTask;
        }

        var offsetsRange = new OffsetsRange { Start = Batch.First().Start, End = Batch.Last().End };

        return _readerSession.CommitOffsetRange(offsetsRange, Batch.First().PartitionId);
    }
}

public class TopicPartitionOffset
{
    public TopicPartitionOffset(long offset, long partitionId)
    {
        Offset = offset;
        PartitionId = partitionId;
    }

    public long Offset { get; }

    public long PartitionId { get; }
}
