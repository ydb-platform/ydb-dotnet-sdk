namespace Ydb.Sdk.Services.Topic;

public class ProducerConfig
{
    public ProducerConfig(Driver driver, string topicPath)
    {
        Driver = driver;
        TopicPath = topicPath;
    }

    public Driver Driver { get; }
    public string TopicPath { get; }
    public string? ProducerId { get; set; }
    public string? MessageGroupId { get; set; }
    public Codec Codec { get; set; } = Codec.Raw; // TODO Supported only Raw
}
