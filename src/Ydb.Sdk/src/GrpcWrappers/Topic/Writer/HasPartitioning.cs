namespace Ydb.Sdk.GrpcWrappers.Topic.Writer;

internal abstract class HasPartitioning<TProto>
{
    //TODO: common request interface in Ydb.Protos?
    protected void SetPartitioningToProto(TProto result, Partitioning? partitioning)
    {
        if (partitioning is null)
            return;

        switch (partitioning.Type)
        {
            case PartitioningType.Undefined:
                SetEmptyPartitioning(result);
                break;
            case PartitioningType.PartitionId:
                SetPartitionId(result, partitioning.PartitionId);
                break;
            case PartitioningType.MessageGroupId:
                SetMessageGroupId(result, partitioning.MessageGroupId);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(partitioning), partitioning, "Unknown partitioning type");
        }
    }

    protected abstract void SetEmptyPartitioning(TProto result);
    protected abstract void SetPartitionId(TProto result, long partitionId);
    protected abstract void SetMessageGroupId(TProto result, string messageGroupId);
}
