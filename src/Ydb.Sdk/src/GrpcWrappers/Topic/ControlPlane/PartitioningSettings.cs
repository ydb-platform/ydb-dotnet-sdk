namespace Ydb.Sdk.GrpcWrappers.Topic.ControlPlane;

internal class PartitioningSettings
{
    public long MinActivePartitions { get; set; }
    public long PartitionCountLimit { get; set; }

    public static PartitioningSettings FromProto(Ydb.Topic.PartitioningSettings settings)
    {
        return new PartitioningSettings
        {
            MinActivePartitions = settings.MinActivePartitions,
            PartitionCountLimit = settings.PartitionCountLimit
        };
    }

    public Ydb.Topic.PartitioningSettings ToProto()
    {
        return new Ydb.Topic.PartitioningSettings
        {
            MinActivePartitions = MinActivePartitions,
            PartitionCountLimit = PartitionCountLimit
        };
    }
}
