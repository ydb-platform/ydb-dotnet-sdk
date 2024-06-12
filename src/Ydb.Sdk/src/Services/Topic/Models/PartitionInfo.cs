namespace Ydb.Sdk.Services.Topic.Models;

public class PartitionInfo
{
    public long Id { get; set; }
    public bool IsActive { get; set; }
    public List<long> ChildPartitions { get; set; } = new();
    public List<long> ParentPartitions { get; set; } = new();

    public static PartitionInfo FromWrapper(PartitionInfo info)
    {
        return new PartitionInfo
        {
            Id = info.Id,
            IsActive = info.IsActive,
            ChildPartitions = info.ChildPartitions,
            ParentPartitions = info.ParentPartitions
        };
    }
}
