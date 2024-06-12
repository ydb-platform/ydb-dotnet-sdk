using Ydb.Topic;

namespace Ydb.Sdk.GrpcWrappers.Topic.Reader.StartPartitionSession;

internal class StartPartitionSessionResponse: ITopicReaderResponse
{
    public long PartitionSessionId { get; set; }
    public long? ReadOffset { get; set; }
    public long? CommitOffset { get; set; }

    public StreamReadMessage.Types.StartPartitionSessionResponse ToProto()
    {
        var result = new StreamReadMessage.Types.StartPartitionSessionResponse
        {
            PartitionSessionId = PartitionSessionId
        };
        if (ReadOffset.HasValue)
            result.ReadOffset = ReadOffset.Value;
        if (CommitOffset.HasValue)
            result.CommitOffset = CommitOffset.Value;

        return result;
    }
}
