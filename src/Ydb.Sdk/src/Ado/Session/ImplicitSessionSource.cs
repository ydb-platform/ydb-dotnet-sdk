namespace Ydb.Sdk.Ado.Session;

internal sealed class ImplicitSessionSource : ISessionSource
{
    private enum State { Open = 0, Closing = 1, Closed = 2 }

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
        if (Volatile.Read(ref _state) == (int)State.Closed)
            return false;

        Interlocked.Increment(ref _activeLeaseCount);

        if (Volatile.Read(ref _state) == (int)State.Closed)
        {
            Interlocked.Decrement(ref _activeLeaseCount);
            return false;
        }

        return true;
    }

    internal void ReleaseLease()
    {
        if (Interlocked.Decrement(ref _activeLeaseCount) == 0 &&
            Volatile.Read(ref _state) != (int)State.Open)
        {
            _allReleased.Set();
        }
    }

    public async ValueTask DisposeAsync()
    {
        var prev = Interlocked.CompareExchange(ref _state, (int)State.Closing, (int)State.Open);
        switch (prev)
        {
            case (int)State.Closed:
                return;
            case (int)State.Closing:
                break;
        }

        if (Volatile.Read(ref _activeLeaseCount) != 0)
            _allReleased.Wait();

        try
        {
            Volatile.Write(ref _state, (int)State.Closed);
            await _driver.DisposeAsync();
        }
        finally
        {
            _allReleased.Dispose();
        }
    }
}
