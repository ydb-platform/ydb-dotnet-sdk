namespace Ydb.Sdk.Services.Topic.Writer;

public class WriterBuilder<TValue>
{
    private readonly WriterConfig _config;
    private readonly IDriver _driver;

    public WriterBuilder(IDriver driver, WriterConfig config)
    {
        _driver = driver;
        _config = config;
    }

    public ISerializer<TValue>? Serializer { get; set; }

    public IWriter<TValue> Build()
    {
        return new Writer<TValue>(
            _driver,
            _config,
            Serializer ?? (ISerializer<TValue>)(
                Serializers.DefaultSerializers.TryGetValue(typeof(TValue), out var serializer)
                    ? serializer
                    : throw new YdbWriterException("The serializer is not set")
            )
        );
    }
}
