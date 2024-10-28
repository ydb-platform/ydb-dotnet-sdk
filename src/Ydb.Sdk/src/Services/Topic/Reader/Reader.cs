namespace Ydb.Sdk.Services.Topic.Reader;

internal class Reader<TValue> : IReader<TValue>
{
    public Reader(Driver driver, ReaderConfig config, IDeserializer<TValue> deserializer)
    {
    }

    internal Task Initialize()
    {
        throw new NotImplementedException();
    }

    public Task<TValue> ReadAsync()
    {
        throw new NotImplementedException();
    }

    public Task<Writer.Message<TValue>> ReadMessageAsync()
    {
        throw new NotImplementedException();
    }
}
