namespace Ydb.Sdk.Services.Topic.Models.Reader;

public class ReadSelector
{
    public string TopicPath { get; set; } = null!;
    public List<long> Partitions { get; set; } = new();
    public DateTime ReadFrom { get; set; }
}
