namespace Ydb.Sdk.Coordination;

public class Lease : IAsyncDisposable
{
    public string Name { get; }

    private readonly SessionTransport _sessionTransport;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly SemaphoreSlim _releaseLock = new(1, 1);

    private bool _released;
    private int _disposed;

    internal Lease(string name, SessionTransport sessionTransport)
    {
        Name = name;
        _sessionTransport = sessionTransport;
        _cancellationTokenSource =
            CancellationTokenSource.CreateLinkedTokenSource(sessionTransport.Token);
    }

    public CancellationToken Token => _cancellationTokenSource.Token;

    public async Task Release(CancellationToken cancellationToken = default)
    {
        if (Volatile.Read(ref _disposed) != 0)
            return;

        await _releaseLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_released || Volatile.Read(ref _disposed) != 0)
                return;

            _released = true;

            try
            {
                await _sessionTransport.ReleaseSemaphore(Name, cancellationToken)
                    .ConfigureAwait(false);
            }
            finally
            {
                await _cancellationTokenSource.CancelAsync().ConfigureAwait(false);
            }
        }
        finally
        {
            _releaseLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        await _releaseLock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        try
        {
            if (!_released)
            {
                _released = true;

                try
                {
                    await _sessionTransport.ReleaseSemaphore(Name, CancellationToken.None)
                        .ConfigureAwait(false);
                }
                finally
                {
                    await _cancellationTokenSource.CancelAsync().ConfigureAwait(false);
                }
            }
        }
        finally
        {
            _releaseLock.Release();
            _cancellationTokenSource.Dispose();
            _releaseLock.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
