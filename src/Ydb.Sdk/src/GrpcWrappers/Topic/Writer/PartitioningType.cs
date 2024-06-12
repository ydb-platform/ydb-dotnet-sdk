namespace Ydb.Sdk.GrpcWrappers.Topic.Writer;

public enum PartitioningType
{
    Undefined,
    MessageGroupId,
    PartitionId,
}