using Google.Protobuf.Collections;
using Ydb.Issue;
using Ydb.Sdk.Client;
using Ydb.Topic;
using SupportedCodecs = Ydb.Sdk.GrpcWrappers.Topic.Codecs.SupportedCodecs;

namespace Ydb.Sdk.GrpcWrappers.Topic.Writer.Init;

internal class InitResponse : ResponseWithResultBase<InitResponse.ResultData>, ITopicWriterResponse
{
    private InitResponse(
        StatusIds.Types.StatusCode statusCode,
        RepeatedField<IssueMessage> issues,
        ResultData? result)
        : base(statusCode, issues, result)
    {
    }

    public static InitResponse FromProto(StreamWriteMessage.Types.FromServer response)
        => FromProto(response.Status, response.Issues, response.InitResponse);

    public static InitResponse FromProto(
        StatusIds.Types.StatusCode statusCode,
        RepeatedField<IssueMessage> issues,
        Ydb.Topic.StreamWriteMessage.Types.InitResponse response)
    {
        return new InitResponse(statusCode, issues, ResultData.FromProto(response));
    }

    public class ResultData
    {
        public long LastSequenceNumber { get; set; }
        public string SessionId { get; set; } = null!;
        public long PartitionId { get; set; }
        public SupportedCodecs SupportedCodecs { get; set; } = null!;

        public static ResultData FromProto(Ydb.Topic.StreamWriteMessage.Types.InitResponse response)
        {
            return new ResultData
            {
                LastSequenceNumber = response.LastSeqNo,
                SessionId = response.SessionId,
                PartitionId = response.PartitionId,
                SupportedCodecs = SupportedCodecs.FromProto(response.SupportedCodecs)
            };
        }
    }
}
