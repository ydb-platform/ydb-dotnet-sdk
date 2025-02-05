using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;
using Ydb.Topic;

namespace Ydb.Sdk.Services.Topic.Reader;

internal class InternalBatchMessages
{
    public InternalBatchMessages(
        ByteString data,
        string topic,
        long partitionId,
        string producerId,
        OffsetsRange offsetsRange,
        Timestamp createdAt,
        RepeatedField<MetadataItem> metadataItems,
        long approximatelyBytesSize)
    {
        Data = data;
        Topic = topic;
        PartitionId = partitionId;
        ProducerId = producerId;
        OffsetsRange = offsetsRange;
        CreatedAt = createdAt;
        MetadataItems = metadataItems;
        ApproximatelyBytesSize = approximatelyBytesSize;
    }

    private ByteString Data { get; }
    private string Topic { get; }
    private long PartitionId { get; }
    private string ProducerId { get; }
    private OffsetsRange OffsetsRange { get; }
    private Timestamp CreatedAt { get; }
    private RepeatedField<MetadataItem> MetadataItems { get; }
    private long ApproximatelyBytesSize { get; }

    internal Message<TValue> ToPublicMessage<TValue>(IDeserializer<TValue> deserializer,
        ReaderSession<TValue> readerSession)
    {
        readerSession.TryReadRequestBytes(ApproximatelyBytesSize);

        return new Message<TValue>(
            data: deserializer.Deserialize(Data.ToByteArray()),
            topic: Topic,
            partitionId: PartitionId,
            producerId: ProducerId,
            createdAt: CreatedAt.ToDateTime(),
            metadata: MetadataItems.Select(item => new Metadata(item.Key, item.Value.ToByteArray())).ToImmutableArray(),
            offsetsRange: OffsetsRange,
            readerSession: readerSession,
            approximatelyBytesSize: ApproximatelyBytesSize
        );
    }
}

internal class InternalBatchMessages<TValue>
{
    private readonly StreamReadMessage.Types.ReadResponse.Types.Batch _batch;
    private readonly PartitionSession _partitionSession;
    private readonly IDeserializer<TValue> _deserializer;
    private readonly ReaderSession<TValue> _readerSession;
    private readonly long _approximatelyBatchSizeOriginal;

    private int _startMessageDataIndex = 0;
    private long _approximatelyBatchSize;

    private int OriginalMessageCount => _batch.MessageData.Count;

    internal bool IsActive => _startMessageDataIndex < OriginalMessageCount && _readerSession.IsActive;

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
        _approximatelyBatchSizeOriginal = approximatelyBatchSize;
        _approximatelyBatchSize = approximatelyBatchSize;
    }

    internal bool TryDequeueMessage([MaybeNullWhen(false)] out Message<TValue> message)
    {
        if (!IsActive)
        {
            message = default;
            return false;
        }

        var index = _startMessageDataIndex++;
        var approximatelyMessageBytesSize = Utils
            .CalculateApproximatelyBytesSize(_approximatelyBatchSizeOriginal, OriginalMessageCount, index);
        var messageData = _batch.MessageData[index];

        TValue value;
        try
        {
            value = _deserializer.Deserialize(messageData.Data.ToByteArray());
        }
        catch (Exception e)
        {
            throw new ReaderException("Error when deserializing message data", e);
        }

        _approximatelyBatchSize -= approximatelyMessageBytesSize;

        message = new Message<TValue>(
            value,
            _partitionSession.TopicPath,
            _partitionSession.PartitionId,
            _batch.ProducerId,
            messageData.CreatedAt.ToDateTime(),
            messageData.MetadataItems.Select(item => new Metadata(item.Key, item.Value.ToByteArray()))
                .ToImmutableArray(),
            new OffsetsRange { Start = _partitionSession.PrevEndOffsetMessage, End = messageData.Offset },
            _readerSession,
            approximatelyMessageBytesSize
        );
        _partitionSession.PrevEndOffsetMessage = messageData.Offset + 1;

        return true;
    }

    internal bool TryPublicBatch([MaybeNullWhen(false)] out BatchMessages<TValue> batchMessages)
    {
        if (!IsActive)
        {
            batchMessages = default;
            return false;
        }

        var offsetsRangeBatch = new OffsetsRange
            { Start = _partitionSession.PrevEndOffsetMessage, End = _batch.MessageData.Last().Offset };
        var approximatelyBatchSize = _approximatelyBatchSize;

        var messages = new List<Message<TValue>>();
        while (TryDequeueMessage(out var message))
        {
            messages.Add(message);
        }

        batchMessages = new BatchMessages<TValue>(messages, _readerSession, approximatelyBatchSize, offsetsRangeBatch);

        return true;
    }
}

internal record CommitSending(
    OffsetsRange OffsetsRange,
    long PartitionSessionId,
    TaskCompletionSource TcsCommit,
    long ApproximatelyBytesSize
);
