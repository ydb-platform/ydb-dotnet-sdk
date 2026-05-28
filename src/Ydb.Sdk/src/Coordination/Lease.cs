namespace Ydb.Sdk.Coordination;

public class Lease : IAsyncDisposable
{
    public string Name { get; }
    private readonly SessionTransport _sessionTransport;
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private int _released;
    private int _disposed;

    internal Lease(string name, SessionTransport sessionTransport)
    {
        Name = name;
        _sessionTransport = sessionTransport;
        //_cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(semaphore);
    }


    public CancellationToken Token => _cancellationTokenSource.Token;

    public async Task Release(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _released, 1) != 0)
            return;

        try
        {
            await _sessionTransport.ReleaseSemaphore(Name, cancellationToken).ConfigureAwait(false);
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
