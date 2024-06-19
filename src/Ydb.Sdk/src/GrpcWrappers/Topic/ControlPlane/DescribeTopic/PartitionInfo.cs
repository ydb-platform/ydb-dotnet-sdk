namespace Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.DescribeTopic;

public class PartitionInfo
{
    public long PartitionId { get; set; }
    public bool IsActive { get; set; }
    public List<long> ChildPartitionIds { get; set; } = null!;
    public List<long> ParentPartitionIds { get; set; } = null!;
    
    public static PartitionInfo FromProto(Ydb.Topic.DescribeTopicResult.Types.PartitionInfo info)
    {
        return new PartitionInfo
        {
            PartitionId = info.PartitionId,
            IsActive = info.Active,
            ChildPartitionIds = info.ChildPartitionIds.Select(id => id).ToList(),
            ParentPartitionIds = info.ParentPartitionIds.Select(id => id).ToList()
        };
    }
}
