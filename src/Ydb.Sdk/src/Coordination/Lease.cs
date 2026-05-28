namespace Ydb.Sdk.Coordination;

public class Lease : IAsyncDisposable
{
    private readonly Semaphore _semaphore;
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private int _released;
    private int _disposed;

    public Lease(Semaphore semaphore)
    {
        _semaphore = semaphore;
    }

    public string GetSemaphoreName()
        => _semaphore.Name;

    public CancellationToken Token => _cancellationTokenSource.Token;

    public async Task Release(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _released, 1) != 0)
            return;

        try
        {
            await _semaphore.Release(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await _cancellationTokenSource.CancelAsync();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

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
