using Ydb.Sdk.GrpcWrappers.Topic.Codecs;
using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.AlterTopic;
using Ydb.Sdk.Services.Topic.Internal.Options;
using Codec = Ydb.Sdk.Services.Topic.Models.Codec;

namespace Ydb.Sdk.Services.Topic.Options;

public class AlterOptions
{
    private readonly List<AlterOption> options;

    private AlterOptions(List<AlterOption> options) => this.options = options;

    public Builder ToBuilder() => new(options);

    public class Builder
    {
        private readonly List<AlterOption> options = new();

        internal Builder()
        {
        }

        internal Builder(List<AlterOption> options) => this.options = options;

        public AlterOptions Build()
        {
            return new AlterOptions(options);
        }

        public void SetMinActivePartitions(long minActivePartitions)
            => options.Add(new AlterOption(r => r.AlterPartitionSettings.MinActivePartitions = minActivePartitions));

        // public void SetMeteringMode(MeteringMode meteringMode)
        //     => options.Add(new AlterOption(r => r.MeteringMode = meteringMode));

        public void SetPartitionCountLimit(long partitionCountLimit)
            => options.Add(new AlterOption(r => r.AlterPartitionSettings.PartitionCountLimit = partitionCountLimit));

        public void SetRetentionPeriod(TimeSpan retentionPeriod)
            => options.Add(new AlterOption(r => r.RetentionPeriod = retentionPeriod));

        public void SetRetentionStorageMb(long retentionStorageMb)
            => options.Add(new AlterOption(r => r.RetentionStorageMb = retentionStorageMb));

        // public void SetSupportedCodecs(List<Codec> codecs)
        //     => options.Add(new AlterOption(r => r.SupportedCodecs = new SupportedCodecs(codecs)));

        public void SetPartitionWriteSpeedBytesPerSecond(long bytesPerSecond)
            => options.Add(new AlterOption(r => r.PartitionWriteSpeedBytesPerSecond = bytesPerSecond));

        public void SetPartitionWriteBurstBytes(long burstBytes)
            => options.Add(new AlterOption(r => r.PartitionWriteBurstBytes = burstBytes));

        public void SetAttributes(Dictionary<string, string> attributes)
            => options.Add(new AlterOption(r => r.AlterAttributes = attributes));

        public void SetConsumersToAdd(IEnumerable<Consumer> consumers)
            => options.Add(new AlterOption(r => r.ConsumersToAdd = consumers.Select(c => c.ToWrapper()).ToList()));

        public void SetConsumersToDrop(List<string> consumers)
            => options.Add(new AlterOption(r => r.ConsumersToDrop = consumers));

        public void SetAlterConsumerImportance(string name, bool isImportant)
        {
            options.Add(new AlterOption(Apply));
            return;

            void Apply(AlterTopicRequest request)
            {
                (request.ConsumersToAlter, var index) = EnsureAlterConsumer(request.ConsumersToAlter, name);
                request.ConsumersToAlter[index].IsImportant = isImportant;
            }
        }

        public void SetAlterConsumerReadFrom(string name, DateTime readFrom)
        {
            options.Add(new AlterOption(Apply));
            return;

            void Apply(AlterTopicRequest request)
            {
                (request.ConsumersToAlter, var index) = EnsureAlterConsumer(request.ConsumersToAlter, name);
                request.ConsumersToAlter[index].ReadFrom = readFrom;
            }
        }

        public void SetAlterConsumerCodecs(string name, List<Codec> codecs)
        {
            options.Add(new AlterOption(Apply));
            return;

            void Apply(AlterTopicRequest request)
            {
                (request.ConsumersToAlter, var index) = EnsureAlterConsumer(request.ConsumersToAlter, name);
                //request.ConsumersToAlter[index].SupportedCodecs = new SupportedCodecs(codecs);
            }
        }

        public void SetAlterConsumerAttributes(string name, Dictionary<string, string> attributes)
        {
            options.Add(new AlterOption(Apply));
            return;

            void Apply(AlterTopicRequest request)
            {
                (request.ConsumersToAlter, var index) = EnsureAlterConsumer(request.ConsumersToAlter, name);
                request.ConsumersToAlter[index].AlterAttributes = attributes;
            }
        }

        private static (List<AlterConsumer> consumers, int index) EnsureAlterConsumer(
            List<AlterConsumer> consumers,
            string consumerName)
        {
            var index = consumers.FindIndex(c => c.Name == consumerName);
            if (index >= 0)
                return (consumers, index);

            var consumer = new AlterConsumer {Name = consumerName};
            consumers.Add(consumer);
            return (consumers, consumers.Count - 1);
        }
    }
}
