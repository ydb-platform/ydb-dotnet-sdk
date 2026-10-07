using System.Diagnostics.CodeAnalysis;
using Ydb.Topic;

namespace Ydb.Sdk.Topic.Reader;

internal class InternalBatchMessages<TValue>(
    IReadOnlyList<StreamReadMessage.Types.ReadResponse.Types.Batch> batches,
    PartitionSession partitionsSession,
    ReaderSession<TValue> readerSession,
    long approximatelyBatchSize,
    IDeserializer<TValue> deserializer,
    long receivedTimestamp)
{
    private readonly int _messageCount = batches.Sum(batch => batch.MessageData.Count);
    private int _batchIndex;
    private int _startMessageDataIndex;
    private int _readMessageCount;

    internal long ReceivedTimestamp { get; } = receivedTimestamp;

    private bool IsActive => partitionsSession.IsActive &&
                             readerSession.IsActive &&
                             _readMessageCount < _messageCount;

    internal bool TryDequeueMessage([MaybeNullWhen(false)] out Message<TValue> message)
    {
        if (!IsActive)
        {
            message = null;
            return false;
        }

        while (_startMessageDataIndex == batches[_batchIndex].MessageData.Count)
        {
            _batchIndex++;
            _startMessageDataIndex = 0;
        }

        var batch = batches[_batchIndex];
        var messageData = batch.MessageData[_startMessageDataIndex++];
        _ = readerSession.TryReadRequestBytes(
            Utils.CalculateApproximatelyBytesSize(approximatelyBatchSize, _messageCount, _readMessageCount++));

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

        var startOffset = partitionsSession.PrevEndOffsetMessage;

        var messages = new List<Message<TValue>>();
        while (TryDequeueMessage(out var message))
        {
            messages.Add(message);
        }

        if (messages.Count == 0)
        {
            batchMessages = null;
            return false;
        }

        batchMessages = new BatchMessages<TValue>(
            batch: messages,
            readerSession: readerSession,
            offsetsRange: new OffsetsRange { Start = startOffset, End = partitionsSession.PrevEndOffsetMessage },
            partitionSessionId: partitionsSession.PartitionSessionId
        );

        return true;
    }
}

internal record CommitSending(
    OffsetsRange OffsetsRange,
    TaskCompletionSource TcsCommit
);
