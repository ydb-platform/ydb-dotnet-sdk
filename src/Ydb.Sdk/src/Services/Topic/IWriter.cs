using Ydb.Sdk.Services.Topic.Writer;

namespace Ydb.Sdk.Services.Topic;

public interface IWriter<TValue> : IDisposable
{
    public Task<WriteResult> WriteAsync(TValue data);

    public Task<WriteResult> WriteAsync(Message<TValue> message);
}
