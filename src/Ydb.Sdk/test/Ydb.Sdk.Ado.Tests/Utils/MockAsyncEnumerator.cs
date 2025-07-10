namespace Ydb.Sdk.Ado.Tests.Utils;

public class MockAsyncEnumerator<T>(IEnumerable<T> items) : IServerStream<T>
{
    private readonly IEnumerator<T> _inner = items.GetEnumerator();

    public T Current => _inner.Current;

    public ValueTask<bool> MoveNextAsync(CancellationToken cancellationToken) => new(_inner.MoveNext());

    public void Dispose()
    {
        GC.SuppressFinalize(this);

        _inner.Dispose();
    }
}
