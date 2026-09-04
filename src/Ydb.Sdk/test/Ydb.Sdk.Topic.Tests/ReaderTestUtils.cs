using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Ydb.Topic;

namespace Ydb.Sdk.Topic.Tests;

using FromServer = StreamReadMessage.Types.FromServer;

internal static class ReaderTestUtils
{
    internal static FromServer InitResponse { get; } = new()
    {
        Status = StatusIds.Types.StatusCode.Success,
        InitResponse = new StreamReadMessage.Types.InitResponse { SessionId = "SessionId" }
    };

    internal static FromServer StartPartitionSessionRequest(
        int committedOffset = 0,
        long partitionSessionId = 1) => new()
        {
            Status = StatusIds.Types.StatusCode.Success,
            StartPartitionSessionRequest = new StreamReadMessage.Types.StartPartitionSessionRequest
            {
                CommittedOffset = committedOffset,
                PartitionOffsets = new OffsetsRange { Start = committedOffset, End = committedOffset + 1000 },
                PartitionSession = new StreamReadMessage.Types.PartitionSession
                { Path = "/topic", PartitionId = partitionSessionId, PartitionSessionId = partitionSessionId }
            }
        };

    internal static FromServer ReadResponse(params byte[][] messages) => ReadResponse(0, messages);

    internal static FromServer ReadResponse(int committedOffset, params byte[][] messages)
    {
        var batch = new StreamReadMessage.Types.ReadResponse.Types.Batch { ProducerId = "ProducerId" };

        foreach (var message in messages)
        {
            batch.MessageData.Add(new StreamReadMessage.Types.ReadResponse.Types.MessageData
            {
                Data = ByteString.CopyFrom(message),
                Offset = committedOffset++,
                CreatedAt = new Timestamp()
            });
        }

        return new FromServer
        {
            Status = StatusIds.Types.StatusCode.Success,
            ReadResponse = new StreamReadMessage.Types.ReadResponse
            {
                BytesSize = 50,
                PartitionData =
                {
                    new StreamReadMessage.Types.ReadResponse.Types.PartitionData
                        { PartitionSessionId = 1, Batches = { batch } }
                }
            }
        };
    }

    internal static FromServer CommitOffsetResponse(int committedOffset = 1) => new()
    {
        Status = StatusIds.Types.StatusCode.Success,
        CommitOffsetResponse = new StreamReadMessage.Types.CommitOffsetResponse
        {
            PartitionsCommittedOffsets =
            {
                new StreamReadMessage.Types.CommitOffsetResponse.Types.PartitionCommittedOffset
                    { PartitionSessionId = 1, CommittedOffset = committedOffset }
            }
        }
    };
}
