namespace Ydb.Sdk.Services.Topic.Models.Reader;

public class PartitionSession
{
    public string Topic { get; set; } = null!;
    public long PartitionId { get; set; }
    public long ReaderId { get; set; }
    public string ConnectionId { get; set; } = null!;
    public long Id { get; set; }
    public long CommittedOffset { get; set; }
}
