using System.Collections.Immutable;
using Google.Protobuf;
using Google.Protobuf.Collections;
using Google.Protobuf.WellKnownTypes;
using Ydb.Topic;

namespace Ydb.Sdk.Services.Topic.Reader;

internal class InternalMessage
{
    public InternalMessage(
        ByteString data,
        string topic,
        long partitionId,
        string producerId,
        OffsetsRange offsetsRange,
        Timestamp createdAt,
        RepeatedField<MetadataItem> metadataItems,
        long dataSize)
    {
        Data = data;
        Topic = topic;
        PartitionId = partitionId;
        ProducerId = producerId;
        OffsetsRange = offsetsRange;
        CreatedAt = createdAt;
        MetadataItems = metadataItems;
        DataSize = dataSize;
    }

    private ByteString Data { get; }

    private string Topic { get; }

    private long PartitionId { get; }

    private string ProducerId { get; }

    private OffsetsRange OffsetsRange { get; }

    private Timestamp CreatedAt { get; }

    private RepeatedField<MetadataItem> MetadataItems { get; }

    private long DataSize { get; }

    internal Message<TValue> ToPublicMessage<TValue>(IDeserializer<TValue> deserializer, ReaderSession readerSession)
    {
        return new Message<TValue>(
            data: deserializer.Deserialize(Data.ToByteArray()),
            topic: Topic,
            partitionId: PartitionId,
            producerId: ProducerId,
            createdAt: CreatedAt.ToDateTime(),
            metadata: MetadataItems.Select(item => new Metadata(item.Key, item.Value.ToByteArray())).ToImmutableArray(),
            offsetsRange: OffsetsRange,
            readerSession: readerSession
        );
    }
}

internal class InternalBatchMessage
{
    public InternalBatchMessage(
        OffsetsRange batchOffsetsRange,
        Queue<InternalMessage> internalMessages,
        ReaderSession readerSession, 
        long approximatelyBatchSize)
    {
        BatchOffsetsRange = batchOffsetsRange;
        InternalMessages = internalMessages;
        ReaderSession = readerSession;
        ApproximatelyBatchSize = approximatelyBatchSize;
    }

    internal OffsetsRange BatchOffsetsRange { get; }

    internal Queue<InternalMessage> InternalMessages { get; }

    internal ReaderSession ReaderSession { get; }

    internal long ApproximatelyBatchSize { get; }
}

internal record CommitSending(
    OffsetsRange OffsetsRange,
    long PartitionSessionId,
    TaskCompletionSource TcsCommit
);
