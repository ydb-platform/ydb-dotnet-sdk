namespace Ydb.Sdk.Services.Topic.Models;

public class PartitionInfo
{
    public long Id { get; set; }
    public bool IsActive { get; set; }
    public List<long> ChildPartitions { get; set; } = new();
    public List<long> ParentPartitions { get; set; } = new();

    public static PartitionInfo FromWrapper(GrpcWrappers.Topic.ControlPlane.DescribeTopic.PartitionInfo info)
    {
        return new PartitionInfo
        {
            Id = info.PartitionId,
            IsActive = info.IsActive,
            ChildPartitions = info.ChildPartitionIds,
            ParentPartitions = info.ParentPartitionIds
        };
    }
}
