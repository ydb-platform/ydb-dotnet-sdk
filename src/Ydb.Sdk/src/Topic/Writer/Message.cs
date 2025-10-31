namespace Ydb.Sdk.Topic.Writer;

public class Message<TValue>
{
    public Message(TValue data)
    {
        Data = data;
    }

    public DateTime Timestamp { get; set; } = DateTime.Now;

    public TValue Data { get; }

    public List<Metadata> Metadata { get; } = new();
}
