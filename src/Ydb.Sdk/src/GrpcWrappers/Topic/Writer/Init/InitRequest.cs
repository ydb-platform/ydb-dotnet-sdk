using static Ydb.Topic.StreamWriteMessage;

namespace Ydb.Sdk.GrpcWrappers.Topic.Writer.Init;

internal class InitRequest : HasPartitioning<Types.InitRequest>
{
    public string Path { get; set; } = null!;
    public string ProducerId { get; set; } = null!;
    public Dictionary<string, string> WriteSessionMeta { get; set; } = null!;
    public Partitioning Partitioning { get; set; } = null!;
    public bool NeedLastSequenceNumber { get; set; }

    public Ydb.Topic.StreamWriteMessage.Types.InitRequest ToProto()
    {
        var request = new Ydb.Topic.StreamWriteMessage.Types.InitRequest
        {
            Path = Path,
            ProducerId = ProducerId,
            GetLastSeqNo = NeedLastSequenceNumber,
        };
        request.WriteSessionMeta.Add(WriteSessionMeta);
        SetPartitioningToProto(request, Partitioning);

        return request;
    }

    protected override void SetEmptyPartitioning(Types.InitRequest result) => result.ClearPartitioning();

    protected override void SetMessageGroupId(Types.InitRequest result, string messageGroupId)
        => result.MessageGroupId = messageGroupId;

    protected override void SetPartitionId(Types.InitRequest result, long partitionId)
        => result.PartitionId = partitionId;
}
