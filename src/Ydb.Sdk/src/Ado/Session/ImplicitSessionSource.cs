namespace Ydb.Sdk.Ado.Session;

internal sealed class ImplicitSessionSource : ISessionSource
{
    private readonly IDriver _driver;
    private readonly Action? _onBecameEmpty;
    private int _isDisposed;
    private int _activeLeaseCount;

    internal ImplicitSessionSource(IDriver driver, Action? onEmpty = null)
    {
        _driver = driver;
        _onBecameEmpty = onEmpty;
    }

    public ValueTask<ISession> OpenSession(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!TryAcquireLease())
            throw new ObjectDisposedException(nameof(ImplicitSessionSource));

        return new ValueTask<ISession>(new ImplicitSession(_driver, this));
    }

    private bool TryAcquireLease()
    {
        if (Volatile.Read(ref _isDisposed) != 0)
            return false;

        Interlocked.Increment(ref _activeLeaseCount);

        if (Volatile.Read(ref _isDisposed) != 0)
        {
            Interlocked.Decrement(ref _activeLeaseCount);
            return false;
        }

        return true;
    }

    internal void ReleaseLease() => Interlocked.Decrement(ref _activeLeaseCount);

    public ValueTask DisposeAsync()
    {
        Interlocked.Exchange(ref _isDisposed, 1);

        var spinner = new SpinWait();
        while (Volatile.Read(ref _activeLeaseCount) != 0)
        {
            spinner.SpinOnce();
        }

        _onBecameEmpty?.Invoke();

        return ValueTask.CompletedTask;
    }
}
