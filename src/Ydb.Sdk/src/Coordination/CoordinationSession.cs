using Microsoft.Extensions.Logging;
using Ydb.Sdk.Coordination.Settings;

namespace Ydb.Sdk.Coordination;

public class CoordinationSession : IAsyncDisposable
{
    private readonly SessionTransport _sessionTransport;

    public CoordinationSession(IDriver driver, ILoggerFactory loggerFactory, string pathNode,
        CancellationToken cancellationToken = default)
    {
        _sessionTransport = new SessionTransport(driver, loggerFactory, pathNode, cancellationToken);
    }

    public CancellationToken Token => _sessionTransport.Token;

    public StateSession Status() => StateSession.Closed;

    public Semaphore Semaphore(string name) => new(name, _sessionTransport);

    public Mutex Mutex(string name) => new(Semaphore(name));

    public Election Election(string name) => new(Semaphore(name));

    public async Task Close()
        => await _sessionTransport.DisposeAsync();

    public async ValueTask DisposeAsync()
    {
        try
        {
            await Close();
        }
        finally
        {
            GC.SuppressFinalize(this);
        }
    }
}
