namespace Ydb.Sdk.Tests;

public class MockAsyncEnumerator<T> : IAsyncEnumerator<T>
{
    private readonly IEnumerator<T> _inner;

    public int Delay { private get; set; }

    public MockAsyncEnumerator(IEnumerable<T> items)
    {
        _inner = items.GetEnumerator();
    }

    public T Current => _inner.Current;

    public async ValueTask<bool> MoveNextAsync()
    {
        await Task.Delay(Delay);
        return _inner.MoveNext();
    }

    public ValueTask DisposeAsync()
    {
        _inner.Dispose();
        return new ValueTask();
    }
}
