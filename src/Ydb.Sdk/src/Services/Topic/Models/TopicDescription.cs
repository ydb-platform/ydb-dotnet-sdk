using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.DescribeTopic;

namespace Ydb.Sdk.Services.Topic.Models;

public class TopicDescription
{
    public string TopicPath { get; set; } = null!;
    public PartitioningSettings PartitioningSettings { get; set; } = new();
    public List<PartitionInfo> PartitionInfos { get; set; } = new();
    public TimeSpan RetentionPeriod;
    public long RetentionStorageMb;
    public List<Codec> SupportedCodecs { get; set; } = new();
    public long PartitionWriteBurstBytes;
    public long PartitionWriteSpeedBytesPerSecond;
    public Dictionary<string, string> Attributes { get; set; } = new();
    public List<Consumer> Consumers { get; set; } = new();
    public MeteringMode MeteringMode;

    internal static TopicDescription FromWrapper(DescribeTopicResponse response)
    {
        return new TopicDescription
        {

        };
    }
}
