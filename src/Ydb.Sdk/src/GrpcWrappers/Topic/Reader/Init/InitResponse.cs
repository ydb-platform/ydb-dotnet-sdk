using Google.Protobuf.Collections;
using Ydb.Issue;
using Ydb.Sdk.Client;
using Ydb.Topic;

namespace Ydb.Sdk.GrpcWrappers.Topic.Reader.Init;

internal class InitResponse: ResponseWithResultBase<InitResponse.ResultData>, ITopicReaderResponse
{
    private InitResponse(
        StatusIds.Types.StatusCode statusCode,
        RepeatedField<IssueMessage> issues,
        ResultData? result)
        : base(statusCode, issues, result)
    {
    }

    public static InitResponse FromProto(StreamReadMessage.Types.FromServer response)
        => FromProto(response.Status, response.Issues, response.InitResponse);

    public static InitResponse FromProto(
        StatusIds.Types.StatusCode statusCode,
        RepeatedField<IssueMessage> issues,
        StreamReadMessage.Types.InitResponse response)
    {
        return new InitResponse(statusCode, issues, ResultData.FromProto(response));
    }

    public class ResultData
    {
        public string SessionId { get; private set; } = null!;

        public static ResultData FromProto(StreamReadMessage.Types.InitResponse response)
        {
            return new ResultData
            {
                SessionId = response.SessionId
            };
        }
    }
}
