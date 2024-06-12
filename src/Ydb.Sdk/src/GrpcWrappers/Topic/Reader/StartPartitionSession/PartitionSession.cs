using Ydb.Topic;

namespace Ydb.Sdk.GrpcWrappers.Topic.Reader.StartPartitionSession;

public class PartitionSession
{
    public long PartitionSessionId { get; private set; }
    public string TopicPath { get; private set; } = null!;
    public long PartitionId { get; private set; }

    public static PartitionSession FromProto(StreamReadMessage.Types.PartitionSession session)
    {
        return new PartitionSession
        {
            PartitionSessionId = session.PartitionSessionId,
            TopicPath = session.Path,
            PartitionId = session.PartitionId
        };
    }
}
