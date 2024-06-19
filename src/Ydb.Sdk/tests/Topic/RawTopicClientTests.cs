using Xunit;
using Ydb.Sdk.GrpcWrappers.Topic;
using Ydb.Sdk.GrpcWrappers.Topic.Codecs;
using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane;
using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.AlterTopic;
using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.CreateTopic;
using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.DescribeTopic;
using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.DropTopic;
using Ydb.Sdk.Tests.Extensions;
using Ydb.Sdk.Tests.Fixture;

namespace Ydb.Sdk.Tests.Topic;

[Trait("Category", "Integration")]
public class RawTopicClientTests : IClassFixture<RawTopicClientFixture>
{
    private readonly RawTopicClient _topicClient;

    public RawTopicClientTests(RawTopicClientFixture fixture)
    {
        _topicClient = fixture.TopicClient;
    }

    [Fact]
    public async Task TestCreateTopic()
    {
        var topicPath = Guid.NewGuid().ToString();
        var request = new CreateTopicRequest
        {
            Path = topicPath,
            RetentionPeriod = TimeSpan.FromDays(1)
        };
        var result = await _topicClient.CreateTopic(request);
        result.Operation.AssertIsSuccess();
    }

    [Fact]
    public async Task TestDropTopic()
    {
        var topicPath = Guid.NewGuid().ToString();
        var createRequest = new CreateTopicRequest
        {
            Path = topicPath,
            RetentionPeriod = TimeSpan.FromDays(1)
        };
        var createResult = await _topicClient.CreateTopic(createRequest);
        createResult.Operation.AssertIsSuccess();
        var dropResult = await _topicClient.DropTopic(new DropTopicRequest {Path = topicPath});
        dropResult.Operation.AssertIsSuccess();
    }

    [Fact]
    public async Task TestDescribeTopic()
    {
        var topicPath = Guid.NewGuid().ToString();
        var createRequest = new CreateTopicRequest
        {
            Path = topicPath,
            PartitionSettings = new PartitioningSettings
            {
                MinActivePartitions = 2,
                PartitionCountLimit = 5
            },
            RetentionPeriod = TimeSpan.FromDays(1),
            RetentionStorageMb = 100,
            SupportedCodecs = new SupportedCodecs(new List<Codec> {Codec.Raw, Codec.Gzip}),
            PartitionWriteSpeedBytesPerSecond = 1,
            PartitionWriteBurstBytes = 5,
            Consumers = new List<Consumer>
            {
                new()
                {
                    Name = "consumer1",
                    SupportedCodecs =
                        new SupportedCodecs(new List<Codec> {Codec.Raw, Codec.Gzip}), //TODO if not specified
                    ReadFrom = DateTime.UtcNow //TODO if not specified //TODO without ms
                }
            }
        };
        var createResult = await _topicClient.CreateTopic(createRequest);
        createResult.Operation.AssertIsSuccess();
        var describeResult = await _topicClient.GetTopicDescription(new DescribeTopicRequest {Path = topicPath});
        describeResult.Operation.AssertIsSuccess();

        //Assert.Equal(createRequest.PartitionSettings, describeResult.PartitioningSettings);
        Assert.Equal(createRequest.RetentionPeriod, describeResult.RetentionPeriod);
        Assert.Equal(createRequest.RetentionStorageMb, describeResult.RetentionStorageMb);
        Assert.Equal(createRequest.SupportedCodecs, describeResult.SupportedCodecs);
        Assert.Equal(createRequest.PartitionWriteSpeedBytesPerSecond, describeResult.PartitionWriteSpeedBytesPerSecond);
        Assert.Equal(createRequest.PartitionWriteBurstBytes, describeResult.PartitionWriteBurstBytes);
        Assert.Equal(createRequest.MeteringMode, describeResult.MeteringMode);
        for (var i = 0; i < createRequest.Consumers.Count; i++)
        {
            AssertConsumer(createRequest.Consumers[i], describeResult.Consumers[i]);
        }
    }

    [Fact]
    public async Task TestAlterTopic()
    {
        var topicPath = Guid.NewGuid().ToString();
        var createRequest = new CreateTopicRequest
        {
            Path = topicPath,
            PartitionSettings = new PartitioningSettings
            {
                MinActivePartitions = 2,
                PartitionCountLimit = 5
            },
            RetentionPeriod = TimeSpan.FromDays(1),
            RetentionStorageMb = 100,
            SupportedCodecs = new SupportedCodecs(new List<Codec> {Codec.Raw, Codec.Gzip}),
            PartitionWriteSpeedBytesPerSecond = 1,
            PartitionWriteBurstBytes = 5,
            Consumers = new List<Consumer>
            {
                new()
                {
                    Name = "consumer1",
                    SupportedCodecs =
                        new SupportedCodecs(new List<Codec> {Codec.Raw, Codec.Gzip}),
                    ReadFrom = DateTime.UtcNow
                },
                new()
                {
                    Name = "consumer2",
                    SupportedCodecs =
                        new SupportedCodecs(new List<Codec> {Codec.Raw, Codec.Gzip}),
                    ReadFrom = DateTime.UtcNow
                }
            },
        };
        var createResult = await _topicClient.CreateTopic(createRequest);
        createResult.Operation.AssertIsSuccess();
        var alterRequest = new AlterTopicRequest
        {
            Path = topicPath,
            AlterPartitionSettings = new AlterPartitioningSettings
            {
                MinActivePartitions = 3,
                PartitionCountLimit = 4
            },
            RetentionPeriod = TimeSpan.FromDays(2),
            RetentionStorageMb = 50,
            SupportedCodecs = new SupportedCodecs(new List<Codec> {Codec.Raw, Codec.Gzip, Codec.Lzop}),
            PartitionWriteSpeedBytesPerSecond = 2,
            PartitionWriteBurstBytes = 10,
            ConsumersToAdd = new List<Consumer>
            {
                new()
                {
                    Name = "consumer3",
                    SupportedCodecs =
                        new SupportedCodecs(new List<Codec> {Codec.Raw, Codec.Gzip, Codec.Lzop}),
                    ReadFrom = DateTime.UtcNow
                }
            },
            ConsumersToDrop = new List<string> {"consumer1"},
            ConsumersToAlter = new List<AlterConsumer>
            {
                new()
                {
                    Name = "consumer2",
                    SupportedCodecs =
                        new SupportedCodecs(new List<Codec>
                            {Codec.Raw, Codec.Gzip, Codec.Lzop}),
                    ReadFrom = DateTime.UtcNow
                }
            }
        };
        var alterResult = await _topicClient.AlterTopic(alterRequest);
        alterResult.Operation.AssertIsSuccess();
        
        var describeResult = await _topicClient.GetTopicDescription(new DescribeTopicRequest {Path = topicPath});
        describeResult.Operation.AssertIsSuccess();

        //TODO assert attributes
        Assert.Equal(alterRequest.RetentionPeriod, describeResult.RetentionPeriod);
        Assert.Equal(alterRequest.RetentionStorageMb, describeResult.RetentionStorageMb);
        Assert.Equal(alterRequest.SupportedCodecs, describeResult.SupportedCodecs);
        Assert.Equal(alterRequest.PartitionWriteSpeedBytesPerSecond, describeResult.PartitionWriteSpeedBytesPerSecond);
        Assert.Equal(alterRequest.PartitionWriteBurstBytes, describeResult.PartitionWriteBurstBytes);
        Assert.Equal(alterRequest.MeteringMode, describeResult.MeteringMode);
        var expectedConsumers = alterRequest.ConsumersToAlter.Select(AlterConsumerToConsumer);
        alterRequest.ConsumersToAdd.ForEach(c => expectedConsumers = expectedConsumers.Append(c));
        foreach (var expectedConsumer in expectedConsumers)
        {
            var actualConsumer = describeResult.Consumers.FirstOrDefault(c => c.Name == expectedConsumer.Name);
            Assert.NotNull(actualConsumer);
            AssertConsumer(expectedConsumer, actualConsumer!);
        }
    }

    private static void AssertConsumer(Consumer expectedConsumer, Consumer actualConsumer)
    {
        Assert.Equal(expectedConsumer.Name, actualConsumer.Name);
        Assert.Equal(expectedConsumer.IsImportant, actualConsumer.IsImportant);
        Assert.True(expectedConsumer.ReadFrom - actualConsumer.ReadFrom < TimeSpan.FromSeconds(1));
        Assert.Equal(expectedConsumer.SupportedCodecs, actualConsumer.SupportedCodecs);
        //Assert.Equal(expectedConsumer.Attributes, actualConsumer.Attributes);
    }

    private static Consumer AlterConsumerToConsumer(AlterConsumer alterConsumer)
    {
        return new Consumer
        {
            Name = alterConsumer.Name,
            IsImportant = alterConsumer.IsImportant ?? false,
            ReadFrom = alterConsumer.ReadFrom,
            SupportedCodecs = alterConsumer.SupportedCodecs ?? new SupportedCodecs(Array.Empty<Codec>()),
            Attributes = alterConsumer.AlterAttributes
        };
    }
}
