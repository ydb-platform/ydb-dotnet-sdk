namespace Ydb.Sdk.Services.Topic.Reader;

public class ReaderBuilder<TValue>
{
    // private readonly ReaderConfig _config;
    // private readonly Driver _driver;
    //
    // public ReaderBuilder(Driver driver, ReaderConfig config)
    // {
    //     _driver = driver;
    //     _config = config;
    // }

    public IDeserializer<TValue>? Deserializer { get; set; }

    public Task<IReader<TValue>> Build()
    {
        throw new NotImplementedException();
        // var reader = new Reader<TValue>(
        //     _driver,
        //     _config,
        //     Deserializer ?? (IDeserializer<TValue>)(
        //         Deserializers.DefaultDeserializers.TryGetValue(typeof(TValue), out var deserializer)
        //             ? deserializer
        //             : throw new YdbWriterException("The serializer is not set")
        //     )
        // );
        //
        // await reader.Initialize();
        //
        // return reader;
    }
}
