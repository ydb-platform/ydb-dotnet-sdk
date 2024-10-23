namespace Ydb.Sdk.Services.Topic;

public class ProducerBuilder<TValue>
{
    private readonly ProducerConfig _config;

    public ProducerBuilder(ProducerConfig config)
    {
        _config = config;
    }

    public ISerializer<TValue>? Serializer { get; set; }

    public async Task<IProducer<TValue>> Build()
    {
        var producer = new Producer<TValue>(
            _config,
            Serializer ?? (ISerializer<TValue>)(
                Serializers.DefaultSerializers.TryGetValue(typeof(TValue), out var serializer)
                    ? serializer
                    : throw new YdbProducerException("The serializer is not set")
            )
        );

        await producer.Initialize();

        return producer;
    }
}
