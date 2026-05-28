using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ydb.Sdk.Coordination.PrimitiveElection;

public class Leadership : IAsyncDisposable
{
    private readonly Semaphore _semaphore;
    private readonly Lease _lease;
    private readonly ILogger<Leadership> _logger;
    private bool _isResigned;

    
    internal Leadership(Semaphore semaphore, Lease lease, ILoggerFactory loggerFactory)
    {
        _semaphore = semaphore;
        _lease = lease;
        _logger = loggerFactory.CreateLogger<Leadership>();
    }

    public async Task Proclaim(byte[]? data, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "Proclaiming leadership on {Name} ({Bytes} bytes)",
            _semaphore.Name,
            data?.Length ?? 0);

        await _semaphore.Update(data, cancellationToken);
    }

    public async Task Resign(CancellationToken cancellationToken = default)
    {
        if (_isResigned)
            return;

        _isResigned = true;

        _logger.LogInformation("Resigning from leadership on {Name}", _semaphore.Name);

        await _lease.Release(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await Resign();
        }
        finally
        {
            GC.SuppressFinalize(this);
        }
    }
}
