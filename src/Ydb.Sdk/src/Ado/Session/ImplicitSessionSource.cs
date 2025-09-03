namespace Ydb.Sdk.Ado.Session;

internal sealed class ImplicitSessionSource : ISessionSource
{
    private readonly IDriver _driver;
    private readonly Action? _onEmpty;
    private int _leased;
    private int _closed;

    internal ImplicitSessionSource(IDriver driver, Action? onEmpty = null)
    {
        _driver = driver;
        _onEmpty = onEmpty;
    }

    public ValueTask<ISession> OpenSession(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (Volatile.Read(ref _closed) == 1)
            throw new ObjectDisposedException(nameof(ImplicitSessionSource));

        Interlocked.Increment(ref _leased);

        if (Volatile.Read(ref _closed) == 1)
        {
            Interlocked.Decrement(ref _leased);
            throw new ObjectDisposedException(nameof(ImplicitSessionSource));
        }

        return new ValueTask<ISession>(new ImplicitSession(_driver, Release));
    }

    private void Release()
    {
        if (Interlocked.Decrement(ref _leased) == 0)
        {
            _onEmpty?.Invoke();
        }
    }

    public ValueTask DisposeAsync()
    {
        Interlocked.Exchange(ref _closed, 1);

        if (Volatile.Read(ref _leased) == 0)
        {
            _onEmpty?.Invoke();
        }

        return ValueTask.CompletedTask;
    }
}
