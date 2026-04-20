using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;

namespace Ydb.Sdk.Coordination;

public class CoordinationSession : IAsyncDisposable
{
    private readonly SessionTransport _sessionTransport;

    public CoordinationSession(IDriver driver, string pathNode, SessionOptions sessionOptions,
        CancellationTokenSource? cancelTokenSource)
    {
        _sessionTransport = new SessionTransport(driver, pathNode, sessionOptions, cancelTokenSource);
    }

    public ulong SessionId() => _sessionTransport.SessionId;
    public StateSession Status() => _sessionTransport.StateSession;
    public CancellationToken Token() => _sessionTransport.Token;


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
