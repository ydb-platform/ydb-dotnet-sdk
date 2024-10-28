namespace Ydb.Sdk.Services.Topic.Reader;

public class ReaderBuilder<TValue>
{
    private readonly ReaderConfig _config;
    private readonly Driver _driver;

    public ReaderBuilder(Driver driver, ReaderConfig config)
    {
        _driver = driver;
        _config = config;
    }
    
    public IDeserializer<TValue>? Deserializer { get; set; }
}
