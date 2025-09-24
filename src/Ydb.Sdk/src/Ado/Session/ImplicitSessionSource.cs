namespace Ydb.Sdk.Ado.Session;

internal sealed class ImplicitSessionSource : ISessionSource
{
    private readonly IDriver _driver;
    private readonly ManualResetEventSlim _allReleased = new(false);

    private int _state;
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
        if (Volatile.Read(ref _state) == 2)
            return false;

        var newCount = Interlocked.Increment(ref _activeLeaseCount);

        var state = Volatile.Read(ref _state);

        if (state == 2 || (state == 1 && newCount == 1))
        {
            Interlocked.Decrement(ref _activeLeaseCount);
            return false;
        }

        return true;
    }

    internal void ReleaseLease()
    {
        if (Interlocked.Decrement(ref _activeLeaseCount) == 0 &&
            Volatile.Read(ref _state) != 0)
        {
            _allReleased.Set();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(ref _state, 1, 0) != 0)
            return;

        if (Volatile.Read(ref _activeLeaseCount) != 0)
            _allReleased.Wait();

        try
        {
            Volatile.Write(ref _state, 2);
            await _driver.DisposeAsync();
        }
        finally
        {
            _allReleased.Dispose();
        }
    }
}
