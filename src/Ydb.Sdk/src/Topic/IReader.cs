using Ydb.Sdk.Topic.Reader;

namespace Ydb.Sdk.Topic;

public interface IReader<TValue> : IAsyncDisposable
{
    public ValueTask<Message<TValue>> ReadAsync(CancellationToken cancellationToken = default);

    public ValueTask<BatchMessages<TValue>> ReadBatchAsync(CancellationToken cancellationToken = default);
}
