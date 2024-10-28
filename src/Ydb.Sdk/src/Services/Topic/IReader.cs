using Ydb.Sdk.Services.Topic.Writer;

namespace Ydb.Sdk.Services.Topic;

public interface IReader<TValue>
{
    public Task<TValue> ReadAsync();

    public Task<Message<TValue>> ReadMessageAsync();
}
