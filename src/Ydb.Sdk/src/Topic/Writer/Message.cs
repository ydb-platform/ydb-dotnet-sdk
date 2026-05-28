namespace Ydb.Sdk.Topic.Writer;

public class Message<TValue>(TValue data)
{
    public DateTime Timestamp { get; set; } = DateTime.Now;

    public TValue Data { get; } = data;

    public List<Metadata> Metadata { get; } = [];
}
