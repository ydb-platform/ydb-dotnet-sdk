using System.Diagnostics.CodeAnalysis;
using Ydb.Topic;

namespace Ydb.Sdk.Topic.Reader;

internal class InternalBatchMessages<TValue>(
    StreamReadMessage.Types.ReadResponse.Types.Batch batch,
    PartitionSession partitionsSession,
    ReaderSession<TValue> readerSession,
    long approximatelyBatchSize,
    IDeserializer<TValue> deserializer)
{
    private int _startMessageDataIndex;

    private int OriginalMessageCount => batch.MessageData.Count;

    private bool IsActive => partitionsSession.IsActive &&
                             readerSession.IsActive &&
                             _startMessageDataIndex < OriginalMessageCount;

    internal bool TryDequeueMessage([MaybeNullWhen(false)] out Message<TValue> message)
    {
        if (!IsActive)
        {
            message = null;
            return false;
        }

        var index = _startMessageDataIndex++;
        var messageData = batch.MessageData[index];
        _ = readerSession.TryReadRequestBytes(
            Utils.CalculateApproximatelyBytesSize(approximatelyBatchSize, OriginalMessageCount, index));

        TValue value;
        try
        {
            value = deserializer.Deserialize(messageData.Data.ToByteArray());
        }
        catch (Exception e)
        {
            throw new ReaderException("Error when deserializing message data", e);
        }

        var nextCommitedOffset = messageData.Offset + 1;

        message = new Message<TValue>(
            data: value,
            topic: partitionsSession.TopicPath,
            partitionId: partitionsSession.PartitionId,
            partitionSessionId: partitionsSession.PartitionSessionId,
            producerId: batch.ProducerId,
            createdAt: messageData.CreatedAt.ToDateTime(),
            metadata: [..messageData.MetadataItems.Select(item => new Metadata(item.Key, item.Value.ToByteArray()))],
            seqNo: messageData.SeqNo,
            offsetsRange: new OffsetsRange
                { Start = partitionsSession.PrevEndOffsetMessage, End = nextCommitedOffset },
            readerSession: readerSession
        );
        partitionsSession.PrevEndOffsetMessage = nextCommitedOffset;

        return true;
    }

    internal bool TryPublicBatch([MaybeNullWhen(false)] out BatchMessages<TValue> batchMessages)
    {
        if (!IsActive)
        {
            batchMessages = null;
            return false;
        }

        var nextCommitedOffset = batch.MessageData.Last().Offset + 1;
        var offsetsRangeBatch = new OffsetsRange
            { Start = partitionsSession.PrevEndOffsetMessage, End = nextCommitedOffset };
        partitionsSession.PrevEndOffsetMessage = nextCommitedOffset;

        var messages = new List<Message<TValue>>();
        while (TryDequeueMessage(out var message))
        {
            messages.Add(message);
        }

        batchMessages = new BatchMessages<TValue>(
            batch: messages,
            readerSession: readerSession,
            offsetsRange: offsetsRangeBatch,
            partitionSessionId: partitionsSession.PartitionSessionId,
            producerId: batch.ProducerId
        );

        return true;
    }
}

internal record CommitSending(
    OffsetsRange OffsetsRange,
    TaskCompletionSource TcsCommit
);
