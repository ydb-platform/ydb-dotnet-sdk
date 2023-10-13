using Ydb.Sdk.Client;

namespace Ydb.Sdk.Services.Query;

public class RetrySettings
{
    public bool IsIdempotent;
}

public partial class QueryClient
{
    public async Task SessionExecStream(
        Func<QuerySession, Task> operationFunc,
        RetrySettings? retrySettings = null)
    {
        await Task.Delay(0);
        throw new NotImplementedException();
    }

    public T SessionExecStream<T>(
        Func<QuerySession, T> operationFunc,
        RetrySettings? retrySettings = null)
        where T : IAsyncEnumerable<IResponse>, IAsyncEnumerator<IResponse>

    {
        throw new NotImplementedException();
    }
}