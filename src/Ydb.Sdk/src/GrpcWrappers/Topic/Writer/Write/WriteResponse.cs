using Google.Protobuf.Collections;
using Ydb.Issue;
using Ydb.Sdk.Client;
using Ydb.Topic;

namespace Ydb.Sdk.GrpcWrappers.Topic.Writer.Write;

internal class WriteResponse : ResponseWithResultBase<WriteResponse.ResultData>, ITopicWriterResponse
{
    private WriteResponse(
        StatusIds.Types.StatusCode statusCode,
        RepeatedField<IssueMessage> issues,
        ResultData? result)
        : base(statusCode, issues, result)
    {
    }

    public static WriteResponse FromProto(StreamWriteMessage.Types.FromServer response)
        => FromProto(response.Status, response.Issues, response.WriteResponse);

    public static WriteResponse FromProto(
        StatusIds.Types.StatusCode statusCode,
        RepeatedField<IssueMessage> issues,
        StreamWriteMessage.Types.WriteResponse response)
    {
        return new WriteResponse(statusCode, issues, ResultData.FromProto(response));
    }

    public class ResultData
    {
        public List<WriteAck> Acks { get; private set; } = new();
        public long PartitionId { get; private set; }
        public WriteStatistics WriteStatistics { get; private set; } = null!;

        public static ResultData FromProto(StreamWriteMessage.Types.WriteResponse response)
        {
            return new ResultData
            {
                Acks = response.Acks.Select(WriteAck.FromProto).ToList(),
                PartitionId = response.PartitionId,
                WriteStatistics = WriteStatistics.FromProto(response.WriteStatistics)
            };
        }
    }
}
