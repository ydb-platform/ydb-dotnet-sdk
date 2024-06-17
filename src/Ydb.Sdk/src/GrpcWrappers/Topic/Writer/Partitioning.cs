namespace Ydb.Sdk.GrpcWrappers.Topic.Writer;

internal class Partitioning
{
    public PartitioningType Type { get; set; }
    public string MessageGroupId { get; set; } = null!;
    public long PartitionId { get; set; }
}
