namespace Ydb.Sdk.Services.Topic.Models.Reader;

public class Batch
{
    public List<Message> Messages { get; set; } = new();
    public CommitRange CommitRange { get; set; } = null!;
}
