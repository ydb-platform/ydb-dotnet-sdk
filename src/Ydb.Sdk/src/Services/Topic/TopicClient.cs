using Google.Protobuf.WellKnownTypes;
using Ydb.Topic;
using Ydb.Topic.V1;

namespace Ydb.Sdk.Services.Topic;

public class TopicClient
{
    private readonly IDriver _driver;

    public TopicClient(IDriver driver)
    {
        _driver = driver;
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

    public async Task DropTopic(DropTopicSettings settings)
    {
        var protoSettings = new DropTopicRequest
        {
            Path = settings.Path,
            OperationParams = settings.MakeOperationParams()
        };

        var response = await _driver.UnaryCall(TopicService.DropTopicMethod, protoSettings, settings);

        Status.FromProto(response.Operation.Status, response.Operation.Issues).EnsureSuccess();
    }
}
