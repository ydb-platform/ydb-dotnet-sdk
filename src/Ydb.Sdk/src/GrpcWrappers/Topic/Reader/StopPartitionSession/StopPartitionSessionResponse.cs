using Ydb.Topic;

namespace Ydb.Sdk.GrpcWrappers.Topic.Reader;

internal class StopPartitionSessionResponse: ITopicReaderResponse
{
    public long PartitionSessionId { get; set; }

    public StreamReadMessage.Types.StopPartitionSessionResponse ToProto()
    {
        return new StreamReadMessage.Types.StopPartitionSessionResponse
        {
            PartitionSessionId = PartitionSessionId
        };
    }
}