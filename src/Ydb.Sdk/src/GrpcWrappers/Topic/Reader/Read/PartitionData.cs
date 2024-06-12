namespace Ydb.Sdk.GrpcWrappers.Topic.Reader.Read;

internal class PartitionData
{
    public long PartitionSessionId { get; set; }
    public List<Batch.Batch> Batches { get; set; } = new();
}
