using Ydb.Sdk.Services.Topic.Internal;
using Ydb.Sdk.Services.Topic.Options;

namespace Ydb.Sdk.Services.Topic;

public class TopicClient
{
    private readonly Driver _driver;

    public TopicClient(Driver driver)
    {
        _driver = driver;
    }

    public TopicWriter StartWriter(string topicPath, WriterOptions options)
    {
        var writerConfig = CreateWriterConfig(options);
        var writerReconnector = new WriterReconnector(_driver, writerConfig);
        return new TopicWriter(writerReconnector);
    }

    private WriterConfig CreateWriterConfig(WriterOptions options)
    {
        throw new NotImplementedException();
    }
}
