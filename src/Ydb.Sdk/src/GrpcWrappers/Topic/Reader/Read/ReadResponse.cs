using Google.Protobuf.Collections;
using Ydb.Issue;
using Ydb.Sdk.Client;
using Ydb.Topic;

namespace Ydb.Sdk.GrpcWrappers.Topic.Reader.Read;

internal class ReadResponse: ResponseWithResultBase<ReadResponse.ResultData>, ITopicReaderResponse
{
    private ReadResponse(
        StatusIds.Types.StatusCode statusCode,
        RepeatedField<IssueMessage> issues,
        ResultData? result)
        : base(statusCode, issues, result)
    {
    }

    public static ReadResponse FromProto(StreamReadMessage.Types.FromServer response)
        => FromProto(response.Status, response.Issues, response.ReadResponse);

    public static ReadResponse FromProto(
        StatusIds.Types.StatusCode statusCode,
        RepeatedField<IssueMessage> issues,
        StreamReadMessage.Types.ReadResponse response)
    {
        return new ReadResponse(statusCode, issues, ResultData.FromProto(response));
    }

    public class ResultData
    {
        public long BytesCount { get; private set; }
        public List<PartitionData> PartitionData { get; private set; } = new();

        public static ResultData FromProto(StreamReadMessage.Types.ReadResponse response)
        {
            return new ResultData
            {
                BytesCount = response.BytesSize,
                //TODO
            };
        }
    }
}
