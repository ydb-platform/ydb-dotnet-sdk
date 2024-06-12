using Ydb.Topic;

namespace Ydb.Sdk.GrpcWrappers.Topic.Reader.CommitOffset;

internal class PartitionCommittedOffset
{
    public long PartitionSessionId { get; private set; }
    public long CommittedOffset { get; private set; }

    public static PartitionCommittedOffset FromProto(
        StreamReadMessage.Types.CommitOffsetRequest.Types.PartitionCommitOffset source)
    {
        return new PartitionCommittedOffset
        {
            PartitionSessionId = source.PartitionSessionId,
            // CommittedOffset = source.
        };
    }
}
