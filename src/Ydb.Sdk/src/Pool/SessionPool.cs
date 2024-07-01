using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Pool;

internal abstract class SessionPool<TSession> where TSession : SessionBase<TSession>
{
    private readonly ILogger<SessionPool<TSession>> _logger;
    private readonly SemaphoreSlim _semaphore;
    private readonly ConcurrentQueue<TSession> _idleSessions = new();

    protected SessionPool(ILogger<SessionPool<TSession>> logger, int? size = null)
    {
        _logger = logger;
        _semaphore = new SemaphoreSlim(size ?? 100);
    }

    public async Task<(Status, TSession?)> GetSession()
    {
        await _semaphore.WaitAsync();

        if (_idleSessions.TryDequeue(out var session) && session.IsActive)
        {
            return (Status.Success, session);
        }

        if (session != null) // not active
        {
            DeleteNotActiveSession(session);
        }

        var (status, newSession) = await CreateSession();

        if (status.IsNotSuccess)
        {
            Release();
        }

        return (status, newSession);
    }

    protected abstract Task<(Status, TSession?)> CreateSession();

    protected abstract Task<Status> DeleteSession(string sessionId);

    public void ReleaseSession(TSession session)
    {
        if (session.IsActive)
        {
            _idleSessions.Enqueue(session);
        }
        else
        {
            DeleteNotActiveSession(session);
        }

        Release();
    }

    private void Release()
    {
        _semaphore.Release();
    }

    private void DeleteNotActiveSession(TSession session)
    {
        _ = DeleteSession(session.SessionId).ContinueWith(s =>
            _logger.LogDebug("Session[{id}] removed with status {status}", session.SessionId, s.Result)
        );
    }
}

public abstract class SessionBase<T> where T : SessionBase<T>
{
    private readonly SessionPool<T> _sessionPool;

    public string SessionId { get; }
    internal long NodeId { get; }

    internal volatile bool IsActive = true;

    internal SessionBase(SessionPool<T> sessionPool, string sessionId, long nodeId)
    {
        _sessionPool = sessionPool;
        SessionId = sessionId;
        NodeId = nodeId;
    }

    internal void OnStatus(Status status)
    {
        if (status.StatusCode is StatusCode.BadSession or StatusCode.SessionBusy or StatusCode.InternalError
            or StatusCode.ClientTransportTimeout or StatusCode.Unavailable or StatusCode.ClientTransportUnavailable)
        {
            IsActive = false;
        }
    }

    internal void Release()
    {
        _sessionPool.ReleaseSession((T)this);
    }
}
