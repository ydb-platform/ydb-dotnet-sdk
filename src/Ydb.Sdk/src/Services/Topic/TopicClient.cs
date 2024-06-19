using Ydb.Sdk.GrpcWrappers.Topic;
using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.AlterTopic;
using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.CreateTopic;
using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.DescribeTopic;
using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.DropTopic;
using Ydb.Sdk.Services.Topic.Internal;
using Ydb.Sdk.Services.Topic.Models;
using Ydb.Sdk.Services.Topic.Options;

namespace Ydb.Sdk.Services.Topic;

public class TopicClient
{
    private readonly Driver _driver;
    private readonly RawTopicClient _rawTopicClient;

    public TopicClient(Driver driver)
    {
        _driver = driver;
        _rawTopicClient = new RawTopicClient(driver);
    }

    public async Task AlterAsync(string path, AlterOptions options)
    {
        var request = new AlterTopicRequest
        {
            Path = path
        };
        options.Options.ForEach(option => option.Apply(request));
        await _rawTopicClient.AlterTopic(request);
    }

    public async Task CreateAsync(string path, CreateOptions options)
    {
        var request = new CreateTopicRequest();
        options.Options.ForEach(option => option.Apply(request));

        await _rawTopicClient.CreateTopic(request);
    }

    public async Task<TopicDescription> GetDescriptionAsync(string path)
    {
        var request = new DescribeTopicRequest
        {
            Path = path
        };
        var result = await _rawTopicClient.GetTopicDescription(request);
        return TopicDescription.FromWrapper(result);
    }

    public async Task DropAsync(string path)
    {
        var request = new DropTopicRequest
        {
            Path = path
        };
        await _rawTopicClient.DropTopic(request);
    }

    public TopicWriter StartWriter(string topicPath, WriterOptions? options = null)
    {
        var writerConfig = CreateWriterConfig(topicPath, options);
        var writerReconnector = new WriterReconnector(_driver, writerConfig);
        return new TopicWriter(writerReconnector);
    }

    private static WriterConfig CreateWriterConfig(string topicPath, WriterOptions? options)
    {
        var config = new WriterConfig
        {
            Topic = topicPath
        };
        options?.Options.ForEach(opt => opt.Apply(config));
        return config;
    }
}
