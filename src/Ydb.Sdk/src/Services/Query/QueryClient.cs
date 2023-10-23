using Ydb.Sdk.Client;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query;

public class RetrySettings
{
    public bool IsIdempotent;
}

public enum TxMode
{
    SerializableRW,
    OnlineRO,
    StaleRO
}

public class QueryClient : IDisposable
{
    public QueryClient(Driver driver)
    {
        throw new NotImplementedException();
    }

    public void Dispose()
    {
        // should rollback if not commit
        throw new NotImplementedException();
    }

    public async Task SessionExecStream(
        Func<QuerySession, Task> operationFunc,
        RetrySettings? retrySettings = null)
    {
        await Task.Delay(0);
        throw new NotImplementedException();
    }

    public T SessionExecStream<T>(
        Func<QuerySession, T> func,
        RetrySettings? retrySettings = null)
        where T : IAsyncEnumerable<IResponse>, IAsyncEnumerator<IResponse>

    {
        throw new NotImplementedException();
    }

    public async Task<T> SessionExec<T>(
        Func<QuerySession, Task<T>> func,
        RetrySettings? retrySettings = null)
    {
        throw new NotImplementedException();
    }

    public async Task<T> SessionExecTx<T>(Func<QuerySession, Tx, T> func)
    {
        throw new NotImplementedException();
    }

    public async Task<Tx> BeginTx()
    {
        throw new NotImplementedException();
    }

    public async Task<T> ExecTx<T>(Func<Tx, T> func, TxMode txMode = TxMode.SerializableRW, bool commit = false)
    {
        throw new NotImplementedException();
    }

    public async Task<T> Query<T>(string query, Dictionary<string, YdbValue> parameters, Func<ExecuteQueryStream, T> func, TxMode txType = TxMode.SerializableRW)
    {
        throw new NotImplementedException();
    }

    public async Task<T> Tx<T>(Func<Tx, T> func, TxMode txType = TxMode.SerializableRW)
    {
        throw new NotImplementedException();
    }
}