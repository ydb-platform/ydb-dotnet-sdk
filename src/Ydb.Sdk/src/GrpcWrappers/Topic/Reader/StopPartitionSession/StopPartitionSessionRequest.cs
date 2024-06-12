using Google.Protobuf.Collections;
using Ydb.Issue;
using Ydb.Sdk.Client;
using Ydb.Topic;
using static Ydb.Topic.StreamReadMessage;

namespace Ydb.Sdk.GrpcWrappers.Topic.Reader.StopPartitionSession;

internal class StopPartitionSessionRequest:
    ResponseWithResultBase<StopPartitionSessionRequest.ResultData>,
    ITopicReaderResponse
{
    private StopPartitionSessionRequest(
        StatusIds.Types.StatusCode statusCode,
        RepeatedField<IssueMessage> issues,
        ResultData? result)
        : base(statusCode, issues, result)
    {
    }

    public static StopPartitionSessionRequest FromProto(Types.FromServer response)
        => FromProto(response.Status, response.Issues, response.StopPartitionSessionRequest);

    public static StopPartitionSessionRequest FromProto(
        StatusIds.Types.StatusCode statusCode,
        RepeatedField<IssueMessage> issues,
        StreamReadMessage.Types.StopPartitionSessionRequest response)
    {
        return new StopPartitionSessionRequest(statusCode, issues, ResultData.FromProto(response));
    }

    public class ResultData
    {
        public long PartitionSessionId { get; private set; }
        public bool IsGraceful { get; private set; }
        public long CommittedOffset { get; private set; }

        public static ResultData FromProto(Types.StopPartitionSessionRequest request)
        {
            return new ResultData
            {
                PartitionSessionId = request.PartitionSessionId,
                IsGraceful = request.Graceful,
                CommittedOffset = request.CommittedOffset,
            };
        }
    }
}