namespace Ydb.Sdk.Ado.Tests.Utils;

public class MockAsyncEnumerator<T>(IEnumerable<T> items) : IServerStream<T>
{
    private readonly IEnumerator<T> _inner = items.GetEnumerator();

    public T Current => _inner.Current;

    public Task<bool> MoveNextAsync(CancellationToken cancellationToken) => Task.FromResult(_inner.MoveNext());

    public void Dispose()
    {
        GC.SuppressFinalize(this);

        _inner.Dispose();
    }
}
