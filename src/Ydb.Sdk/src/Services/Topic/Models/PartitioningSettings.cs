namespace Ydb.Sdk.Services.Topic.Models;

public class PartitioningSettings
{
    public long MinActivePartitions { get; set; }
    public long PartitionCountLimit { get; set; }

    internal static PartitioningSettings FromWrapper(GrpcWrappers.Topic.ControlPlane.PartitioningSettings settings)
    {
        return new PartitioningSettings
        {
            MinActivePartitions = settings.MinActivePartitions,
            PartitionCountLimit = settings.PartitionCountLimit
        };
    }

    internal GrpcWrappers.Topic.ControlPlane.PartitioningSettings ToWrapper()
    {
        return new GrpcWrappers.Topic.ControlPlane.PartitioningSettings
        {
            MinActivePartitions = MinActivePartitions,
            PartitionCountLimit = PartitionCountLimit
        };
    }
}
