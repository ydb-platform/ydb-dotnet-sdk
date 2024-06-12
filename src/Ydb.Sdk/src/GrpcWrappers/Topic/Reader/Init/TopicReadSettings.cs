using Ydb.Sdk.GrpcWrappers.Extensions;
using Ydb.Topic;

namespace Ydb.Sdk.GrpcWrappers.Topic.Reader.Init;

internal class TopicReadSettings
{
    public string Path { get; set; } = null!;
    public List<long> PartitionsIds { get; set; } = new();
    public TimeSpan? MaxLag { get; set; }
    public DateTime? ReadFrom { get; set; }

    public StreamReadMessage.Types.InitRequest.Types.TopicReadSettings ToProto()
    {
        var result = new StreamReadMessage.Types.InitRequest.Types.TopicReadSettings
        {
            MaxLag = MaxLag.ToDuration(),
            ReadFrom = ReadFrom.ToTimestamp(),
            Path = Path
        };
        result.PartitionIds.AddRange(PartitionsIds);

        return result;
    }
}
