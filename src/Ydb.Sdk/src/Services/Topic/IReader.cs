using Ydb.Sdk.Services.Topic.Reader;

namespace Ydb.Sdk.Services.Topic;

public interface IReader<TValue> : IDisposable
{
    public ValueTask<Message<TValue>> ReadAsync(CancellationToken cancellationToken = default);

    public ValueTask<BatchMessage<TValue>> ReadBatchAsync(CancellationToken cancellationToken = default);
}
