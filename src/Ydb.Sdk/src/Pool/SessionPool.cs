using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Pool;

internal abstract class SessionPool<TSession> where TSession : SessionBase<TSession>
{
    private readonly ILogger<SessionPool<TSession>> _logger;
    private readonly SemaphoreSlim _semaphoreSlim;
    private readonly ConcurrentQueue<TSession> _idleSessions = new();

    protected SessionPool(ILogger<SessionPool<TSession>> logger, int? size = null)
    {
        _logger = logger;
        _semaphoreSlim = new SemaphoreSlim(size ?? 100);
    }

    public async Task<(Status, TSession?)> GetSession()
    {
        await _semaphoreSlim.WaitAsync();

        if (_idleSessions.TryDequeue(out var session) && session.IsActive)
        {
            return (Status.Success, session);
        }

        if (session != null) // not active
        {
            _ = DeleteSession().ContinueWith(s =>
                _logger.LogDebug("Session[{id}] removed with status {status}", session.SessionId, s)
            );
        }

        var (status, newSession) = await CreateSession();

        if (status.IsNotSuccess)
        {
            Release();
        }

        return (status, newSession);
    }

    protected abstract Task<(Status, TSession?)> CreateSession();

    protected abstract Task<Status> DeleteSession();

    public void ReleaseSession(TSession session)
    {
        _idleSessions.Enqueue(session);

        Release();
    }

    public void Release()
    {
        _semaphoreSlim.Release();
    }
}

public abstract class SessionBase<T> where T : SessionBase<T>
{
    private readonly SessionPool<T> _sessionPool;

    public string SessionId { get; }
    public int NodeId { get; }

    internal volatile bool IsActive = true;

    internal SessionBase(SessionPool<T> sessionPool, string sessionId, int nodeId)
    {
        _sessionPool = sessionPool;
        SessionId = sessionId;
        NodeId = nodeId;
    }

    internal void OnStatus(Status status)
    {
        if (status.StatusCode is StatusCode.BadSession or StatusCode.SessionBusy or StatusCode.InternalError
            or StatusCode.ClientTransportTimeout or StatusCode.Unavailable)
        {
            IsActive = false;
        }
    }

    internal void Release()
    {
        _sessionPool.ReleaseSession((T)this);
    }
}
