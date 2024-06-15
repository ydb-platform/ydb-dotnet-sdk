using Ydb.Sdk.GrpcWrappers.Topic;
using Ydb.Sdk.Services.Topic.Internal;
using Ydb.Sdk.Services.Topic.Options;

namespace Ydb.Sdk.Services.Topic;

public class TopicClient
{
    private readonly TopicConfig config;
    private readonly RawTopicClient rawClient;
    private readonly Driver driver;

    public TopicClient(Driver driver)
    {
        rawClient = new RawTopicClient(driver);
    }

    public TopicWriter StartWriter(string topicPath, WriterOptions options)
    {
        var writerConfig = CreateWriterConfig(options);
        var writerReconnector = new WriterReconnector(driver, writerConfig);
        return new TopicWriter(writerReconnector);
    }

    private WriterConfig CreateWriterConfig(WriterOptions options)
    {
        throw new NotImplementedException();
    }
}
