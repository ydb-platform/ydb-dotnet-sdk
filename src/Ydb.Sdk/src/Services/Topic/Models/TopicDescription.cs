using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.DescribeTopic;
using Ydb.Sdk.Utils;

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

    internal static TopicDescription FromWrapper(DescribeTopicResult wrapper)
    {
        return new TopicDescription
        {
            TopicPath = wrapper.Self.Name,
            PartitioningSettings = PartitioningSettings.FromWrapper(wrapper.PartitioningSettings),
            PartitionInfos = wrapper.Partitions.Select(PartitionInfo.FromWrapper).ToList(),
            RetentionPeriod = wrapper.RetentionPeriod,
            RetentionStorageMb = wrapper.RetentionStorageMb,
            SupportedCodecs = wrapper.SupportedCodecs.ToPublic().ToList(),
            PartitionWriteBurstBytes = wrapper.PartitionWriteBurstBytes,
            PartitionWriteSpeedBytesPerSecond = wrapper.PartitionWriteSpeedBytesPerSecond,
            Attributes = wrapper.Attributes,
            Consumers = wrapper.Consumers.Select(Consumer.FromWrapper).ToList(),
            MeteringMode = EnumConverter.Convert<
                GrpcWrappers.Topic.ControlPlane.MeteringMode,
                MeteringMode>(
                wrapper.MeteringMode)
        };
    }
}
