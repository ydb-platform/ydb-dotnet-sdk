using Ydb.Sdk.Services.Topic.Reader;

namespace Ydb.Sdk.Services.Topic;

public interface IReader<TValue> : IDisposable
{
    public ValueTask<Message<TValue>> ReadAsync(CancellationToken cancellationToken = default);

    public ValueTask<BatchMessages<TValue>> ReadBatchAsync(CancellationToken cancellationToken = default);
}
