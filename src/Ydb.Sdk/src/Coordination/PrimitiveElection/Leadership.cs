using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ydb.Sdk.Coordination.PrimitiveElection;

public class Leadership : IAsyncDisposable
{
    public string Name { get; }
    private readonly SessionTransport _sessionTransport;
    private readonly Lease _lease;
    private readonly ILogger<Leadership> _logger;
    private bool _isResigned;


    internal Leadership(string name, SessionTransport sessionTransport, Lease lease, ILoggerFactory loggerFactory)
    {
        Name = name;
        _sessionTransport = sessionTransport;
        _lease = lease;
        _logger = loggerFactory.CreateLogger<Leadership>();
    }

    public async Task Proclaim(byte[]? data, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "Proclaiming leadership on {Name} ({Bytes} bytes)",
            Name,
            data?.Length ?? 0);

        await _sessionTransport.UpdateSemaphore(Name, data, cancellationToken);
    }

    public async Task Resign(CancellationToken cancellationToken = default)
    {
        if (_isResigned)
            return;

        _isResigned = true;

        _logger.LogInformation("Resigning from leadership on {Name}", Name);

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
