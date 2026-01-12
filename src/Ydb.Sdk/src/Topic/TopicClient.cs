using Google.Protobuf.WellKnownTypes;
using Ydb.Sdk.Ado;
using Ydb.Topic;
using Ydb.Topic.V1;
using static Ydb.Sdk.Ado.PoolManager;

namespace Ydb.Sdk.Topic;

/// <summary>
/// Client for YDB Topic service operations.
/// </summary>
/// <remarks>
/// TopicClient provides methods for managing YDB topics including creation, modification,
/// and deletion of topics and their configurations.
/// </remarks>
public class TopicClient : IAsyncDisposable
{
    private readonly IDriver _driver;

    /// <summary>
    /// Initializes a new instance of the <see cref="TopicClient"/> class.
    /// </summary>
    /// <param name="connectionString">The connectionString to use for topic operations.</param>
    public TopicClient(string connectionString)
    {
        _driver = GetDriver(new YdbConnectionStringBuilder(connectionString)).AsTask().Result;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="TopicClient"/> class.
    /// </summary>
    /// <param name="ydbConnectionStringBuilder">The ydbConnectionStringBuilder to use for topic operations.</param>
    public TopicClient(YdbConnectionStringBuilder ydbConnectionStringBuilder)
    {
        _driver = GetDriver(ydbConnectionStringBuilder).AsTask().Result;
    }

    public async Task CreateTopic(CreateTopicSettings settings)
    {
        var protoSettings = new CreateTopicRequest
        {
            Path = settings.Path,
            RetentionStorageMb = settings.RetentionStorageMb,
            PartitionWriteBurstBytes = settings.PartitionWriteBurstBytes,
            PartitionWriteSpeedBytesPerSecond = settings.PartitionWriteSpeedBytesPerSecond,
            MeteringMode = (Ydb.Topic.MeteringMode)settings.MeteringMode,
            OperationParams = settings.MakeOperationParams()
        };

        protoSettings.Attributes.Add(settings.Attributes);

        if (settings.PartitioningSettings != null)
        {
            protoSettings.PartitioningSettings = new Ydb.Topic.PartitioningSettings
            {
                MinActivePartitions = settings.PartitioningSettings.MinActivePartitions,
                MaxActivePartitions = settings.PartitioningSettings.MaxActivePartitions
            };
        }

        if (settings.RetentionPeriod != null)
        {
            protoSettings.RetentionPeriod = Duration.FromTimeSpan(settings.RetentionPeriod.Value);
        }

        foreach (var codec in settings.SupportedCodecs)
        {
            protoSettings.SupportedCodecs.Codecs.Add((int)codec);
        }

        foreach (var consumer in settings.Consumers)
        {
            var protoConsumer = new Ydb.Topic.Consumer
            {
                Name = consumer.Name,
                Important = consumer.Important
            };
            protoConsumer.Attributes.Add(consumer.Attributes);

            if (consumer.ReadFrom != null)
            {
                protoConsumer.ReadFrom = Timestamp.FromDateTime(consumer.ReadFrom.Value);
            }

            foreach (var codec in consumer.SupportedCodecs)
            {
                protoConsumer.SupportedCodecs.Codecs.Add((int)codec);
            }

            protoSettings.Consumers.Add(protoConsumer);
        }

        var response = await _driver.UnaryCall(TopicService.CreateTopicMethod, protoSettings, settings);

        Status.FromProto(response.Operation.Status, response.Operation.Issues).EnsureSuccess();
    }

    public async Task DropTopic(string topicName, GrpcRequestSettings? settings = null)
    {
        var protoSettings = new DropTopicRequest
        {
            Path = topicName
        };

        var response = await _driver.UnaryCall(TopicService.DropTopicMethod, protoSettings,
            settings ?? new GrpcRequestSettings());

        Status.FromProto(response.Operation.Status, response.Operation.Issues).EnsureSuccess();
    }

    public ValueTask DisposeAsync() => _driver.DisposeAsync();
}
