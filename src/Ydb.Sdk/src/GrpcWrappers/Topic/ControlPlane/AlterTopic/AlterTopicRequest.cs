using Ydb.Sdk.GrpcWrappers.Topic.Codecs;
using Ydb.Sdk.GrpcWrappers.Topic.Extensions;
using Ydb.Sdk.Utils;

namespace Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.AlterTopic;

internal class AlterTopicRequest
{
    public string Path { get; set; } = null!;
    public AlterPartitioningSettings AlterPartitionSettings { get; set; } = new(); //TODO set?
    public TimeSpan? RetentionPeriod { get; set; }
    public long? RetentionStorageMb { get; set; }
    public SupportedCodecs? SupportedCodecs { get; set; }
    public long? PartitionWriteSpeedBytesPerSecond {get; set; }
    public long? PartitionWriteBurstBytes {get; set; }
    public Dictionary<string, string> AlterAttributes { get; set; } = new();
    public List<Consumer> ConsumersToAdd {get; set; } = new();
    public List<string> ConsumersToDrop {get; set; } = new();
    public List<AlterConsumer> ConsumersToAlter {get; set; } = new();
    public MeteringMode MeteringMode {get; set; }
    public OperationSettings OperationParameters { get; set; } = new();

    public Ydb.Topic.AlterTopicRequest ToProto()
    {
        var result =  new Ydb.Topic.AlterTopicRequest
        {
            Path = Path,
            OperationParams = OperationParameters.MakeOperationParams(),
            AlterPartitioningSettings = AlterPartitionSettings.ToProto(),
            SetMeteringMode = EnumConverter.Convert<MeteringMode, Ydb.Topic.MeteringMode>(MeteringMode),
            AlterAttributes = {AlterAttributes},
            AddConsumers = {ConsumersToAdd.Select(c => c.ToProto())},
            DropConsumers = {ConsumersToDrop},
            AlterConsumers = {ConsumersToAlter.Select(c => c.ToProto())}
        };

        if (RetentionPeriod.HasValue)
            result.SetRetentionPeriod = RetentionPeriod.ToDuration();
        if (RetentionStorageMb.HasValue)
            result.SetRetentionStorageMb = RetentionStorageMb.Value;
        if (PartitionWriteSpeedBytesPerSecond.HasValue)
            result.SetPartitionWriteSpeedBytesPerSecond = PartitionWriteSpeedBytesPerSecond.Value;
        if (PartitionWriteBurstBytes.HasValue)
            result.SetPartitionWriteBurstBytes = PartitionWriteBurstBytes.Value;
        if (SupportedCodecs != null)
            result.SetSupportedCodecs = SupportedCodecs.ToProto();

        return result;
    }
}
