using Ydb.Sdk.GrpcWrappers.Topic.Extensions;
using Ydb.Sdk.Services.Scheme;
using Ydb.Sdk.Utils;
using SupportedCodecs = Ydb.Sdk.GrpcWrappers.Topic.Codecs.SupportedCodecs;

namespace Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.DescribeTopic;

internal class DescribeTopicResult
{
    public SchemeEntry Self { get; set; } = null!;
    public PartitioningSettings PartitioningSettings { get; set; } = new();
    public List<PartitionInfo> Partitions { get; set; } = new();
    public TimeSpan RetentionPeriod { get; set; }
    public long RetentionStorageMb { get; set; }
    public SupportedCodecs SupportedCodecs { get; set; } = null!;
    public long PartitionWriteSpeedBytesPerSecond {get; set; }
    public long PartitionWriteBurstBytes {get; set; }
    public Dictionary<string, string> Attributes { get; set; } = new();
    public List<Consumer> Consumers {get; set; } = new();
    public MeteringMode MeteringMode {get; set; }

    public static DescribeTopicResult FromProto(Ydb.Topic.DescribeTopicResponse response)
    {
        var result = response.Operation.Result.Unpack<Ydb.Topic.DescribeTopicResult>();

        return new DescribeTopicResult
        {
            Self = SchemeEntry.FromProto(result.Self),
            PartitioningSettings = PartitioningSettings.FromProto(result.PartitioningSettings),
            Partitions = result.Partitions.Select(PartitionInfo.FromProto).ToList(),
            RetentionPeriod = result.RetentionPeriod.ToTimeSpan(),
            RetentionStorageMb = result.RetentionStorageMb,
            SupportedCodecs = SupportedCodecs.FromProto(result.SupportedCodecs),
            PartitionWriteSpeedBytesPerSecond = result.PartitionWriteSpeedBytesPerSecond,
            PartitionWriteBurstBytes = result.PartitionWriteBurstBytes,
            Attributes = result.Attributes.ToDictionary(),
            Consumers = result.Consumers.Select(Consumer.FromProto).ToList(),
            MeteringMode = EnumConverter.Convert<Ydb.Topic.MeteringMode, MeteringMode>(result.MeteringMode)
        };
    }
}
