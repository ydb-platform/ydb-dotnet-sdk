using Ydb.Sdk.Services.Topic.Models;

namespace Ydb.Sdk.Services.Topic.Options;

public class WriterOptions
{
    internal List<WriterOption> Options { get; }

    private WriterOptions(List<WriterOption> options) => Options = options;

    public Builder ToBuilder() => new(Options);

    public class Builder
    {
        private readonly List<WriterOption> _options = new();

        internal Builder(List<WriterOption> options) => _options = options;

        public WriterOptions Build()
        {
            return new WriterOptions(_options);
        }

        public void SetProducerId(string producerId)
            => _options.Add(new WriterOption(config => config.ProducerId = producerId));

        public void SetManualMessageSequenceNumberIncrement()
            => _options.Add(new WriterOption(config => config.AutoSetSequenceNumber = false));

        public void SetManualMessageCreationTimeControl()
            => _options.Add(new WriterOption(config => config.AutoSetSequenceNumber = false));

        public void SetCodec(Codec codec)
            => _options.Add(new WriterOption(config => config.Codec = codec));

        public void SetEncoders(Dictionary<Codec, Func<byte[], byte[]>> encoders)
            => _options.Add(new WriterOption(config => config.Encoders = encoders));
    }
}
