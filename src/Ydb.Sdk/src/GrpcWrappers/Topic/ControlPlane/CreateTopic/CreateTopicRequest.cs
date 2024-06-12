using Google.Protobuf.WellKnownTypes;
using Ydb.Sdk.GrpcWrappers.Topic.Codecs;
using Ydb.Sdk.Utils;

namespace Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.CreateTopic;

internal class CreateTopicRequest
{
    public string Path { get; set; }
    public PartitioningSettings PartitionSettings { get; set; }
    public TimeSpan RetentionPeriod { get; set; }
    public long RetentionStorageMb { get; set; }
    public SupportedCodecs SupportedCodecs { get; set; } = null!;
    public long PartitionWriteSpeedBytesPerSecond { get; set; }
    public long PartitionWriteBurstBytes { get; set; }
    public Dictionary<string, string> Attributes { get; set; } = new();
    public List<Consumer> Consumers { get; set; } = new();
    public MeteringMode MeteringMode { get; set; }
    public OperationSettings OperationParameters { get; set; } = new();

    public Ydb.Topic.CreateTopicRequest ToProto()
    {
        return new Ydb.Topic.CreateTopicRequest
        {
            Path = Path,
            OperationParams = OperationParameters.MakeOperationParams(),
            PartitioningSettings = PartitionSettings.ToProto(),
            RetentionPeriod = RetentionPeriod.ToDuration(),
            RetentionStorageMb = RetentionStorageMb,
            SupportedCodecs = SupportedCodecs.ToProto(),
            PartitionWriteSpeedBytesPerSecond = PartitionWriteSpeedBytesPerSecond,
            PartitionWriteBurstBytes = PartitionWriteBurstBytes,
            Attributes = {Attributes},
            Consumers = {Consumers.Select(c => c.ToProto())},
            MeteringMode = EnumConverter.Convert<MeteringMode, Ydb.Topic.MeteringMode>(MeteringMode)
        };
    }
}
