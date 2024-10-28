namespace Ydb.Sdk.Services.Topic.Reader;

internal class Reader<TValue> : IReader<TValue>
{
    public Task<TValue> Read()
    {
        throw new NotImplementedException();
    }

    public Task<Writer.Message<TValue>> ReadMessage()
    {
        throw new NotImplementedException();
    }
}
