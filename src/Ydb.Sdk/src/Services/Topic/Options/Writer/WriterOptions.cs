using Ydb.Sdk.Services.Topic.Models;

// ReSharper disable once CheckNamespace
namespace Ydb.Sdk.Services.Topic.Options;

public class WriterOptions
{
    private WriterOptions(List<WriterOption> options) => Options = options;
    internal List<WriterOption> Options { get; }

    public Builder ToBuilder() => new(Options);

    public class Builder
    {
        private readonly List<WriterOption> _options = new();

        internal Builder()
        {
        }

        internal Builder(List<WriterOption> options) => _options = options;

        public WriterOptions Build()
        {
            return new WriterOptions(_options);
        }

        public Builder SetProducerId(string producerId)
        {
            _options.Add(new WriterOption(config => config.ProducerId = producerId));
            return this;
        }

        public Builder SetManualMessageSequenceNumberIncrement()
        {
            _options.Add(new WriterOption(config => config.AutoSetSequenceNumber = false));
            return this;
        }

        public Builder SetManualMessageCreationTimeControl()
        {
            _options.Add(new WriterOption(config => config.AutoSetSequenceNumber = false));
            return this;
        }

        public Builder SetCodec(Codec codec)
        {
            _options.Add(new WriterOption(config => config.Codec = codec));
            return this;
        }

        public Builder SetEncoders(Dictionary<Codec, Func<byte[], byte[]>> encoders)
        {
            _options.Add(new WriterOption(config => config.Encoders = encoders));
            return this;
        }
    }
}
