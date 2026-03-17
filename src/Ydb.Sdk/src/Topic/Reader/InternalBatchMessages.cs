using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using Ydb.Topic;

namespace Ydb.Sdk.Topic.Reader;

internal class InternalBatchMessages<TValue>
{
    private readonly long _approximatelyBatchSize;
    private readonly StreamReadMessage.Types.ReadResponse.Types.Batch _batch;
    private readonly IDeserializer<TValue> _deserializer;
    private readonly PartitionSession _partitionSession;
    private readonly ReaderSession<TValue> _readerSession;

    private int _startMessageDataIndex;

    public InternalBatchMessages(
        StreamReadMessage.Types.ReadResponse.Types.Batch batch,
        PartitionSession partitionsSession,
        ReaderSession<TValue> readerSession,
        long approximatelyBatchSize,
        IDeserializer<TValue> deserializer)
    {
        _batch = batch;
        _partitionSession = partitionsSession;
        _readerSession = readerSession;
        _deserializer = deserializer;
        _approximatelyBatchSize = approximatelyBatchSize;
    }

    private int OriginalMessageCount => _batch.MessageData.Count;

    private bool IsActive => _partitionSession.IsActive &&
                             _readerSession.IsActive &&
                             _startMessageDataIndex < OriginalMessageCount;

    internal bool TryDequeueMessage([MaybeNullWhen(false)] out Message<TValue> message)
    {
        if (!IsActive)
        {
            message = null;
            return false;
        }

        var index = _startMessageDataIndex++;
        var messageData = _batch.MessageData[index];
        _ = _readerSession.TryReadRequestBytes(
            Utils.CalculateApproximatelyBytesSize(_approximatelyBatchSize, OriginalMessageCount, index));

        TValue value;
        try
        {
            value = _deserializer.Deserialize(messageData.Data.ToByteArray());
        }
        catch (Exception e)
        {
            throw new ReaderException("Error when deserializing message data", e);
        }

        var nextCommitedOffset = messageData.Offset + 1;

        message = new Message<TValue>(
            value,
            _partitionSession.TopicPath,
            _partitionSession.PartitionId,
            _partitionSession.PartitionSessionId,
            _batch.ProducerId,
            messageData.CreatedAt.ToDateTime(),
            messageData.MetadataItems.Select(item => new Metadata(item.Key, item.Value.ToByteArray()))
                .ToImmutableArray(),
            messageData.SeqNo,
            new OffsetsRange
                { Start = _partitionSession.PrevEndOffsetMessage, End = nextCommitedOffset },
            _readerSession
        );
        _partitionSession.PrevEndOffsetMessage = nextCommitedOffset;

        return true;
    }

    internal bool TryPublicBatch([MaybeNullWhen(false)] out BatchMessages<TValue> batchMessages)
    {
        if (!IsActive)
        {
            batchMessages = null;
            return false;
        }

        var nextCommitedOffset = _batch.MessageData.Last().Offset + 1;
        var offsetsRangeBatch = new OffsetsRange
            { Start = _partitionSession.PrevEndOffsetMessage, End = nextCommitedOffset };
        _partitionSession.PrevEndOffsetMessage = nextCommitedOffset;

        var messages = new List<Message<TValue>>();
        while (TryDequeueMessage(out var message)) messages.Add(message);

        batchMessages = new BatchMessages<TValue>(
            messages,
            _readerSession,
            offsetsRangeBatch,
            _partitionSession.PartitionSessionId,
            _batch.ProducerId
        );

        return true;
    }
}

internal record CommitSending(
    OffsetsRange OffsetsRange,
    TaskCompletionSource TcsCommit
);
