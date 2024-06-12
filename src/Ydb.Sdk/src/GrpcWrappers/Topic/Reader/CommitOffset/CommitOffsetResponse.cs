using Google.Protobuf.Collections;
using Ydb.Issue;
using Ydb.Sdk.Client;
using Ydb.Topic;

namespace Ydb.Sdk.GrpcWrappers.Topic.Reader.CommitOffset;

internal class CommitOffsetResponse: ResponseWithResultBase<CommitOffsetResponse.ResultData>, ITopicReaderResponse
{
    private CommitOffsetResponse(
        StatusIds.Types.StatusCode statusCode,
        RepeatedField<IssueMessage> issues,
        ResultData? result)
        : base(statusCode, issues, result)
    {
    }

    public static CommitOffsetResponse FromProto(StreamReadMessage.Types.FromServer response)
        => FromProto(response.Status, response.Issues, response.CommitOffsetResponse);

    public static CommitOffsetResponse FromProto(
        StatusIds.Types.StatusCode statusCode,
        RepeatedField<IssueMessage> issues,
        StreamReadMessage.Types.CommitOffsetResponse response)
    {
        return new CommitOffsetResponse(statusCode, issues, ResultData.FromProto(response));
    }

    public class ResultData
    {
        public List<PartitionCommittedOffset> PartitionsCommittedOffsets { get; private set; } = new();

        public static ResultData FromProto(StreamReadMessage.Types.CommitOffsetResponse response)
        {
            return new ResultData
            {
                // PartitionsCommittedOffsets = response.PartitionsCommittedOffsets.ToList()
            };
        }
    }
}
