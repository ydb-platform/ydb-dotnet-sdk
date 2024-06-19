using Ydb.Sdk.GrpcWrappers.Topic.Codecs;
using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.AlterTopic;
using Ydb.Sdk.Services.Topic.Models;
using Ydb.Sdk.Utils;
using Codec = Ydb.Sdk.Services.Topic.Models.Codec;

// ReSharper disable once CheckNamespace
namespace Ydb.Sdk.Services.Topic.Options;

public class AlterOptions
{
    private AlterOptions(List<AlterOption> options) => Options = options;

    internal List<AlterOption> Options { get; }

    public Builder ToBuilder() => new(Options);

    public class Builder
    {
        private readonly List<AlterOption> _options = new();

        internal Builder()
        {
        }

        internal Builder(List<AlterOption> options) => this._options = options;

        public AlterOptions Build()
        {
            return new AlterOptions(_options);
        }

        public void SetMinActivePartitions(long minActivePartitions)
            => _options.Add(new AlterOption(r => r.AlterPartitionSettings.MinActivePartitions = minActivePartitions));

        public void SetMeteringMode(MeteringMode meteringMode)
        {
            _options.Add(new AlterOption(r => r.MeteringMode = EnumConverter.Convert<
                MeteringMode,
                GrpcWrappers.Topic.ControlPlane.MeteringMode>(
                meteringMode)));
        }

        public void SetPartitionCountLimit(long partitionCountLimit)
            => _options.Add(new AlterOption(r => r.AlterPartitionSettings.PartitionCountLimit = partitionCountLimit));

        public void SetRetentionPeriod(TimeSpan retentionPeriod)
            => _options.Add(new AlterOption(r => r.RetentionPeriod = retentionPeriod));

        public void SetRetentionStorageMb(long retentionStorageMb)
            => _options.Add(new AlterOption(r => r.RetentionStorageMb = retentionStorageMb));

        public void SetSupportedCodecs(List<Codec> codecs)
            => _options.Add(new AlterOption(r => r.SupportedCodecs = SupportedCodecs.FromPublic(codecs)));

        public void SetPartitionWriteSpeedBytesPerSecond(long bytesPerSecond)
            => _options.Add(new AlterOption(r => r.PartitionWriteSpeedBytesPerSecond = bytesPerSecond));

        public void SetPartitionWriteBurstBytes(long burstBytes)
            => _options.Add(new AlterOption(r => r.PartitionWriteBurstBytes = burstBytes));

        public void SetAttributes(Dictionary<string, string> attributes)
            => _options.Add(new AlterOption(r => r.AlterAttributes = attributes));

        public void SetConsumersToAdd(IEnumerable<Consumer> consumers)
            => _options.Add(new AlterOption(r => r.ConsumersToAdd = consumers.Select(c => c.ToWrapper()).ToList()));

        public void SetConsumersToDrop(List<string> consumers)
            => _options.Add(new AlterOption(r => r.ConsumersToDrop = consumers));

        public void SetAlterConsumerImportance(string name, bool isImportant)
        {
            _options.Add(new AlterOption(Apply));
            return;

            void Apply(AlterTopicRequest request)
            {
                (request.ConsumersToAlter, var index) = EnsureAlterConsumer(request.ConsumersToAlter, name);
                request.ConsumersToAlter[index].IsImportant = isImportant;
            }
        }

        public void SetAlterConsumerReadFrom(string name, DateTime readFrom)
        {
            _options.Add(new AlterOption(Apply));
            return;

            void Apply(AlterTopicRequest request)
            {
                (request.ConsumersToAlter, var index) = EnsureAlterConsumer(request.ConsumersToAlter, name);
                request.ConsumersToAlter[index].ReadFrom = readFrom;
            }
        }

        public void SetAlterConsumerCodecs(string name, List<Codec> codecs)
        {
            _options.Add(new AlterOption(Apply));
            return;

            void Apply(AlterTopicRequest request)
            {
                (request.ConsumersToAlter, var index) = EnsureAlterConsumer(request.ConsumersToAlter, name);
                request.ConsumersToAlter[index].SupportedCodecs = SupportedCodecs.FromPublic(codecs);
            }
        }

        public void SetAlterConsumerAttributes(string name, Dictionary<string, string> attributes)
        {
            _options.Add(new AlterOption(Apply));
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
