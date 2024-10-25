using System.Text;

namespace Ydb.Sdk.Services.Topic;

public class WriterConfig
{
    public WriterConfig(Driver driver, string topicPath)
    {
        Driver = driver;
        TopicPath = topicPath;
    }

    public Driver Driver { get; }
    public string TopicPath { get; }
    public string? ProducerId { get; set; }
    public string? MessageGroupId { get; set; }
    public Codec Codec { get; set; } = Codec.Raw; // TODO Supported only Raw

    public override string ToString()
    {
        var toString = new StringBuilder().Append("[TopicPath: ").Append(TopicPath);

        if (ProducerId != null)
        {
            toString.Append(", ProducerId: ").Append(ProducerId);
        }

        if (MessageGroupId != null)
        {
            toString.Append(", MessageGroupId: ").Append(MessageGroupId);
        }

        return toString.Append(", Codec: ").Append(Codec).Append(']').ToString();
    }
}
