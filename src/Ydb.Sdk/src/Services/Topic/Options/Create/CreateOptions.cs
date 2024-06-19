using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane;
using Ydb.Sdk.GrpcWrappers.Topic.ControlPlane.CreateTopic;
using Ydb.Sdk.Utils;

using Codec = Ydb.Sdk.Services.Topic.Models.Codec;
using Consumer = Ydb.Sdk.Services.Topic.Models.Consumer;
using MeteringMode = Ydb.Sdk.Services.Topic.Models.MeteringMode;
using SupportedCodecs = Ydb.Sdk.GrpcWrappers.Topic.Codecs.SupportedCodecs;
// ReSharper disable once CheckNamespace

namespace Ydb.Sdk.Services.Topic.Options;

public class CreateOptions
{
    private CreateOptions(List<CreateOption> options) => Options = options;

    internal List<CreateOption> Options { get; }

    public Builder ToBuilder() => new(Options);

    public class Builder
    {
        private readonly List<CreateOption> _options = new();

        internal Builder()
        {
        }

        internal Builder(List<CreateOption> options) => _options = options;

        public CreateOptions Build()
        {
            return new CreateOptions(_options);
        }

        public void SetMinActivePartitions(long minActivePartitions)
        {
            _options.Add(new CreateOption(AddPartitionCountLimit));
            return;

            void AddPartitionCountLimit(CreateTopicRequest request)
            {
                request.PartitionSettings ??= new PartitioningSettings();
                request.PartitionSettings.MinActivePartitions = minActivePartitions;
            }
        }

        public void SetMeteringMode(MeteringMode meteringMode)
        {
            _options.Add(new CreateOption(r => r.MeteringMode = EnumConverter.Convert<
                MeteringMode,
                GrpcWrappers.Topic.ControlPlane.MeteringMode>(
                meteringMode)));
        }

        public void SetPartitionCountLimit(long partitionCountLimit)
        {
            _options.Add(new CreateOption(AddPartitionCountLimit));
            return;

            void AddPartitionCountLimit(CreateTopicRequest request)
            {
                request.PartitionSettings ??= new PartitioningSettings();
                request.PartitionSettings.PartitionCountLimit = partitionCountLimit;
            }
        }

        public void SetRetentionPeriod(TimeSpan retentionPeriod)
            => _options.Add(new CreateOption(r => r.RetentionPeriod = retentionPeriod));

        public void SetRetentionStorageMb(long retentionStorageMb)
            => _options.Add(new CreateOption(r => r.RetentionStorageMb = retentionStorageMb));

        public void SetSupportedCodecs(List<Codec> codecs)
            => _options.Add(new CreateOption(r => r.SupportedCodecs = SupportedCodecs.FromPublic(codecs)));

        public void SetPartitionWriteSpeedBytesPerSecond(long bytesPerSecond)
            => _options.Add(new CreateOption(r => r.PartitionWriteSpeedBytesPerSecond = bytesPerSecond));

        public void SetPartitionWriteBurstBytes(long burstBytes)
            => _options.Add(new CreateOption(r => r.PartitionWriteBurstBytes = burstBytes));

        public void SetAttributes(Dictionary<string, string> attributes)
            => _options.Add(new CreateOption(r => r.Attributes = attributes));

        public void SetConsumers(IEnumerable<Consumer> consumers)
            => _options.Add(new CreateOption(r => r.Consumers = consumers.Select(c => c.ToWrapper()).ToList()));
    }
}
