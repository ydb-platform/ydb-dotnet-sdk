namespace Ydb.Sdk.Tests;

public class MockAsyncEnumerator<T> : IServerStream<T>
{
    private readonly IEnumerator<T> _inner;

    public MockAsyncEnumerator(IEnumerable<T> items)
    {
        _inner = items.GetEnumerator();
    }

    public T Current => _inner.Current;

    public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken) => new(_inner.MoveNext());

    public void Dispose()
    {
        GC.SuppressFinalize(this);

        _inner.Dispose();
    }
}
