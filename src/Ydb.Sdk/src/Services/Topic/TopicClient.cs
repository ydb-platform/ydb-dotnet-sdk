using Ydb.Sdk.GrpcWrappers.Topic;
using Ydb.Sdk.Services.Topic.Internal;
using Ydb.Sdk.Services.Topic.Models.Reader;
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

    public Task AlterAsync(string path, AlterOptions options)
    {
        throw new NotImplementedException();
    }

    public Task CreateAsync(string path, CreateOptions options)
    {
        throw new NotImplementedException();
    }

    // public Task<TopicDescription> DescribeAsync(string path, DescribeOptions options)
    public Task GetDescriptionAsync(string path, DescribeOptions options)
    {
        throw new NotImplementedException();
    }

    public Task DropAsync(string path, DropOptions options)
    {
        throw new NotImplementedException();
    }

    public TopicReader StartReader(string consumer, List<ReadSelector> readSelectors, ReaderOptions options)
    {
        var readerConfig = CreateReaderConfig(consumer, readSelectors, options);
        var readerReconnector = new ReaderReconnector(driver, readerConfig);
        return new TopicReader(readerReconnector);
    }

    public TopicWriter StartWriter(string topicPath, WriterOptions options)
    {
        var writerConfig = CreateWriterConfig(options);
        var writerReconnector = new WriterReconnector(driver, writerConfig);
        return new TopicWriter(writerReconnector);
    }

    private ReaderConfig CreateReaderConfig(string consumer, List<ReadSelector> readSelectors, ReaderOptions options)
    {
        throw new NotImplementedException();
    }

    private WriterConfig CreateWriterConfig(WriterOptions options)
    {
        throw new NotImplementedException();
    }
}
