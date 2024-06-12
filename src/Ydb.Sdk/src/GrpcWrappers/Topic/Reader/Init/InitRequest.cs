using Ydb.Topic;

namespace Ydb.Sdk.GrpcWrappers.Topic.Reader.Init;

internal class InitRequest
{
    public List<TopicReadSettings> TopicReadSettings { get; set; } = null!;
    public string Consumer { get; set; } = null!;

    public StreamReadMessage.Types.InitRequest ToProto()
    {
        var result = new StreamReadMessage.Types.InitRequest
        {
            Consumer = Consumer,
        };
        result.TopicsReadSettings.AddRange(TopicReadSettings.Select(s => s.ToProto()));

        return result;
    }
}
