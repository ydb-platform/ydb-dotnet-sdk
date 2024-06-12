namespace Ydb.Sdk.Services.Topic.Models.Reader;

public class CommitRange
{
    public long CommitOffsetStart { get; set; }
    public long CommitOffsetEnd { get; set; }
    public PartitionSession PartitionSession { get; set; } = null!;
}
