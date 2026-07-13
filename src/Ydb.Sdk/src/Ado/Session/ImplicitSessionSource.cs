using Microsoft.Extensions.Logging;
using Ydb.Sdk.Internal;

namespace Ydb.Sdk.Ado.Session;

internal sealed class ImplicitSessionSource : ISessionSource
{
    private const int DisposeTimeoutSeconds = 10;

    private readonly ILogger _logger;
    private readonly string? _frameworkClientInfo;
    private readonly TaskCompletionSource _drainedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private int _isDisposed;
    private int _activeLeaseCount;

    public IDriver Driver { get; }
    public YdbMetricsReporter MetricsReporter { get; }

    internal ImplicitSessionSource(IDriver driver, YdbConnectionStringBuilder settings)
    {
        Driver = driver;
        _logger = settings.LoggerFactory.CreateLogger<ImplicitSessionSource>();
        MetricsReporter = new YdbMetricsReporter(settings);
        _frameworkClientInfo = settings.ClientInfo;
    }

    public ValueTask<ISession> OpenSession(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        return TryAcquireLease()
            ? new ValueTask<ISession>(new ImplicitSession(Driver, this))
            : throw new ObjectDisposedException(nameof(ImplicitSessionSource),
                "The implicit session source has been closed.");
    }

    private bool TryAcquireLease()
    {
        Interlocked.Increment(ref _activeLeaseCount);

        if (Volatile.Read(ref _isDisposed) == 0)
            return true;

        ReleaseLease();
        return false;
    }

    internal void ReleaseLease()
    {
        Interlocked.Decrement(ref _activeLeaseCount);

        if (Volatile.Read(ref _isDisposed) == 1 && _activeLeaseCount == 0)
            _drainedTcs.TrySetResult();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(ref _isDisposed, 1, 0) != 0)
            return;

        MetricsReporter.Dispose();

        try
        {
            if (Volatile.Read(ref _activeLeaseCount) != 0)
            {
                await _drainedTcs.Task.WaitAsync(TimeSpan.FromSeconds(DisposeTimeoutSeconds)).ConfigureAwait(false);
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
                await Driver.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Failed to dispose the transport driver");
            }

            SdkClientInfoRegistry.Unregister(Metadata.AdoNetClientInfo);
            if (_frameworkClientInfo is not null)
            {
                SdkClientInfoRegistry.Unregister(_frameworkClientInfo);
            }
        }
    }
}
