using Google.Protobuf.Collections;
using Ydb.Issue;
using Ydb.Sdk.Client;
using Ydb.Topic;

namespace Ydb.Sdk.GrpcWrappers.Topic.Reader.StartPartitionSession;

internal class StartPartitionSessionRequest:
    ResponseWithResultBase<StartPartitionSessionRequest.ResultData>,
    ITopicReaderResponse
{
    private StartPartitionSessionRequest(
        StatusIds.Types.StatusCode statusCode,
        RepeatedField<IssueMessage> issues,
        ResultData? result)
        : base(statusCode, issues, result)
    {
    }

    public static StartPartitionSessionRequest FromProto(StreamReadMessage.Types.FromServer response)
        => FromProto(response.Status, response.Issues, response.StartPartitionSessionRequest);

    public static StartPartitionSessionRequest FromProto(
        StatusIds.Types.StatusCode statusCode,
        RepeatedField<IssueMessage> issues,
        StreamReadMessage.Types.StartPartitionSessionRequest response)
    {
        return new StartPartitionSessionRequest(statusCode, issues, ResultData.FromProto(response));
    }

    public class ResultData
    {
        public PartitionSession PartitionSession { get; private set; } = null!;
        public long CommittedOffset { get; private set; }
        public OffsetsRange PartitionOffsets { get; private set; } = null!;

        public static ResultData FromProto(StreamReadMessage.Types.StartPartitionSessionRequest request)
        {
            return new ResultData
            {
                CommittedOffset = request.CommittedOffset,
                PartitionSession = PartitionSession.FromProto(request.PartitionSession),
                PartitionOffsets = OffsetsRange.FromProto(request.PartitionOffsets)
            };
        }
    }
}