using Google.Protobuf.WellKnownTypes;
using Ydb.Sdk.GrpcWrappers.Topic.Codecs;
using Ydb.Sdk.Utils;

namespace Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.CreateTopic;

internal class CreateTopicRequest
{
    public string Path { get; set; } = null!;
    public PartitioningSettings? PartitionSettings { get; set; }
    public TimeSpan RetentionPeriod { get; set; } //TODO required
    public long RetentionStorageMb { get; set; }
    public SupportedCodecs? SupportedCodecs { get; set; }
    public long PartitionWriteSpeedBytesPerSecond { get; set; }
    public long PartitionWriteBurstBytes { get; set; }
    public Dictionary<string, string> Attributes { get; set; } = new();
    public List<Consumer> Consumers { get; set; } = new();
    public MeteringMode MeteringMode { get; set; }
    public OperationSettings OperationSettings { get; set; } = new();

    public Ydb.Topic.CreateTopicRequest ToProto()
    {
        return new Ydb.Topic.CreateTopicRequest
        {
            Path = Path,
            OperationParams = OperationSettings.MakeOperationParams(),
            PartitioningSettings = PartitionSettings?.ToProto() ?? new Ydb.Topic.PartitioningSettings(),
            RetentionPeriod = RetentionPeriod.ToDuration(),
            RetentionStorageMb = RetentionStorageMb,
            SupportedCodecs = SupportedCodecs?.ToProto() ?? new Ydb.Topic.SupportedCodecs(), //TODO
            PartitionWriteSpeedBytesPerSecond = PartitionWriteSpeedBytesPerSecond,
            PartitionWriteBurstBytes = PartitionWriteBurstBytes,
            Attributes = {Attributes},
            Consumers = {Consumers.Select(c => c.ToProto())},
            MeteringMode = EnumConverter.Convert<MeteringMode, Ydb.Topic.MeteringMode>(MeteringMode)
        };
    }
}
