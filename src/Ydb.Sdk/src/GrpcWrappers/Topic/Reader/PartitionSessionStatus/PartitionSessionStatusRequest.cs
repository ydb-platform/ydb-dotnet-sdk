using Ydb.Topic;

namespace Ydb.Sdk.GrpcWrappers.Topic.Reader.PartitionSessionStatus;

internal class PartitionSessionStatusRequest
{
    public long PartitionSessionId { get; set; }

    public StreamReadMessage.Types.PartitionSessionStatusRequest ToProto()
    {
        return new StreamReadMessage.Types.PartitionSessionStatusRequest
        {
            PartitionSessionId = PartitionSessionId
        };
    }
}
