namespace Ydb.Sdk.Coordination;

// BAD LEASE
public class Lease : IAsyncDisposable
{
    private readonly Semaphore _semaphore;
    private readonly CancellationTokenSource _cancellationTokenSource = new();

    public Lease(Semaphore semaphore)
    {
        _semaphore = semaphore;
    }

    public string GetSemaphoreName()
        => _semaphore.Name;

    public CancellationToken Token => _cancellationTokenSource.Token;

    public async Task Release(CancellationToken cancellationToken = default)
    {
        if (_cancellationTokenSource.IsCancellationRequested)
            return;
        try
        {
            await _semaphore.Release(cancellationToken).ConfigureAwait(false);
            await _cancellationTokenSource.CancelAsync();
        }
        finally
        {
            await _cancellationTokenSource.CancelAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await Release(CancellationToken.None);
        }
        finally
        {
            _cancellationTokenSource.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
