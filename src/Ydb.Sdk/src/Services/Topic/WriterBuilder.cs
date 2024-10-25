namespace Ydb.Sdk.Services.Topic;

public class WriterBuilder<TValue>
{
    private readonly WriterConfig _config;
    private readonly Driver _driver;

    public WriterBuilder(Driver driver, WriterConfig config)
    {
        _driver = driver;
        _config = config;
    }

    public ISerializer<TValue>? Serializer { get; set; }

    public async Task<IWriter<TValue>> Build()
    {
        var writer = new Writer<TValue>(
            _driver,
            _config,
            Serializer ?? (ISerializer<TValue>)(
                Serializers.DefaultSerializers.TryGetValue(typeof(TValue), out var serializer)
                    ? serializer
                    : throw new YdbWriterException("The serializer is not set")
            )
        );

        await writer.Initialize();

        return writer;
    }
}
