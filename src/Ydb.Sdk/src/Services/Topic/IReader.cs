using Ydb.Sdk.Services.Topic.Reader;

namespace Ydb.Sdk.Services.Topic;

public interface IReader<TValue>
{
    public Task<TValue> ReadAsync();

    public Task<Message<TValue>> ReadMessageAsync();
}
