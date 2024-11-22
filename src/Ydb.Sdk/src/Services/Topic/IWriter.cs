using Ydb.Sdk.Services.Topic.Writer;

namespace Ydb.Sdk.Services.Topic;

public interface IWriter<TValue> : IDisposable
{
    public Task<WriteResult> WriteAsync(TValue data, CancellationToken cancellationToken = default);

    public Task<WriteResult> WriteAsync(Message<TValue> message, CancellationToken cancellationToken = default);
}
