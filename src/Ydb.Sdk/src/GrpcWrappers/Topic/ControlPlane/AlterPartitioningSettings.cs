namespace Ydb.Sdk.GrpcWrappers.Topic.ControlPlane;

internal class AlterPartitioningSettings
{
    public long? MinActivePartitions { get; set; }
    public long? PartitionCountLimit { get; set; }

    public static AlterPartitioningSettings FromProto(Ydb.Topic.AlterPartitioningSettings settings)
    {
        var result = new AlterPartitioningSettings();
        if (settings.HasSetMinActivePartitions)
            result.MinActivePartitions = settings.SetMinActivePartitions;
        if (settings.HasSetPartitionCountLimit)
            result.PartitionCountLimit = settings.SetPartitionCountLimit;

        return result;
    }

    public Ydb.Topic.AlterPartitioningSettings ToProto()
    {
        var result = new Ydb.Topic.AlterPartitioningSettings();
        if (MinActivePartitions.HasValue)
            result.SetMinActivePartitions = MinActivePartitions.Value;
        if (PartitionCountLimit.HasValue)
            result.SetPartitionCountLimit = PartitionCountLimit.Value;

        return result;
    }
}
