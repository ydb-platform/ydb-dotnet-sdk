using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Ado.Session;

internal sealed class ImplicitSessionSource : ISessionSource
{
    private const int DisposeTimeoutSeconds = 10;

    private readonly IDriver _driver;
    private readonly ILogger _logger;
    private readonly TaskCompletionSource _drainedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private int _isDisposed;
    private int _activeLeaseCount;

    internal ImplicitSessionSource(IDriver driver, ILoggerFactory loggerFactory)
    {
        _driver = driver;
        _logger = loggerFactory.CreateLogger<ImplicitSessionSource>();
    }

    public ValueTask<ISession> OpenSession(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        return TryAcquireLease()
            ? new ValueTask<ISession>(new ImplicitSession(_driver, this))
            : throw new ObjectDisposedException(nameof(ImplicitSessionSource));
    }

    private bool TryAcquireLease()
    {
        if (Volatile.Read(ref _isDisposed) != 0)
            return false;

        Interlocked.Increment(ref _activeLeaseCount);

        if (Volatile.Read(ref _isDisposed) == 0)
            return true;

        ReleaseLease();
        return false;
    }

    internal void ReleaseLease()
    {
        if (Interlocked.Decrement(ref _activeLeaseCount) == 0 && Volatile.Read(ref _isDisposed) == 1)
            _drainedTcs.TrySetResult();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(ref _isDisposed, 1, 0) != 0)
            return;

        try
        {
            if (Volatile.Read(ref _activeLeaseCount) != 0)
            {
                await _drainedTcs.Task.WaitAsync(TimeSpan.FromSeconds(DisposeTimeoutSeconds));
            }
        }
        catch (TimeoutException)
        {
            _logger.LogCritical("Disposal timed out: Some implicit sessions are still active");

            throw new YdbException("Timeout while disposing of the pool: some implicit sessions are still active. " +
                                   "This may indicate a connection leak or suspended operations.");
        }
        finally
        {
            try
            {
                await _driver.DisposeAsync();
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Failed to dispose the transport driver");
            }
        }
    }
}
