namespace Ydb.Sdk.Ado.Session;

internal sealed class ImplicitSessionSource : ISessionSource
{
    private readonly IDriver _driver;
    private readonly Action? _onEmpty;
    private int _leased;

    internal ImplicitSessionSource(IDriver driver, Action? onEmpty = null)
    {
        _driver = driver;
        _onEmpty = onEmpty;
    }

    public ValueTask<ISession> OpenSession(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Interlocked.Increment(ref _leased);

        return new ValueTask<ISession>(new ImplicitSession(_driver, Release));
    }

    private void Release()
    {
        if (Interlocked.Decrement(ref _leased) == 0)
        {
            _onEmpty?.Invoke();
        }
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
