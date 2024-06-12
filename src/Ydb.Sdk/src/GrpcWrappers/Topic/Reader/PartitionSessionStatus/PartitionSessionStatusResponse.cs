using Google.Protobuf.Collections;
using Ydb.Issue;
using Ydb.Sdk.Client;
using Ydb.Topic;
using static Ydb.Topic.StreamReadMessage;

namespace Ydb.Sdk.GrpcWrappers.Topic.Reader.PartitionSessionStatus;

internal class PartitionSessionStatusResponse:
    ResponseWithResultBase<PartitionSessionStatusResponse.ResultData>,
    ITopicReaderResponse
{
    private PartitionSessionStatusResponse(
        StatusIds.Types.StatusCode statusCode,
        RepeatedField<IssueMessage> issues,
        ResultData? result)
        : base(statusCode, issues, result)
    {
    }

    public static PartitionSessionStatusResponse FromProto(Types.FromServer response)
        => FromProto(response.Status, response.Issues, response.PartitionSessionStatusResponse);

    public static PartitionSessionStatusResponse FromProto(
        StatusIds.Types.StatusCode statusCode,
        RepeatedField<IssueMessage> issues,
        StreamReadMessage.Types.PartitionSessionStatusResponse response)
    {
        return new PartitionSessionStatusResponse(statusCode, issues, ResultData.FromProto(response));
    }

    public class ResultData
    {
        public long PartitionSessionId { get; private set; }
        public long CommittedOffset { get; private set; }
        public DateTime WriteTimeHighWatermark { get; private set; }
        public OffsetsRange PartitionOffsets { get; private set; } = null!;

        public static ResultData FromProto(Types.PartitionSessionStatusResponse response)
        {
            return new ResultData
            {
                PartitionSessionId = response.PartitionSessionId,
                CommittedOffset = response.CommittedOffset,
                WriteTimeHighWatermark = response.WriteTimeHighWatermark.ToDateTime(),
                PartitionOffsets = OffsetsRange.FromProto(response.PartitionOffsets)
            };
        }
    }
}
