namespace Ydb.Sdk.Ado.Session;

internal sealed class ImplicitSessionSource : ISessionSource
{
    private readonly IDriver _driver;
    private readonly ManualResetEventSlim _allReleased = new(false);
    private int _isDisposed;
    private int _activeLeaseCount;

    internal ImplicitSessionSource(IDriver driver)
    {
        _driver = driver;
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

        if (Volatile.Read(ref _isDisposed) == 0)
            return true;

        Interlocked.Decrement(ref _activeLeaseCount);
        return false;
    }

    internal void ReleaseLease()
    {
        if (Interlocked.Decrement(ref _activeLeaseCount) == 0 && Volatile.Read(ref _isDisposed) != 0)
            _allReleased.Set();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(ref _isDisposed, 1, 0) != 0)
            return;

        if (Volatile.Read(ref _activeLeaseCount) != 0)
            _allReleased.Wait();

        await _driver.DisposeAsync();
    }
}
