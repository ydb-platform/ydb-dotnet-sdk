using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Ado;
using Exception = System.Exception;

namespace Ydb.Sdk.Pool;

internal abstract class SessionPool<TSession> where TSession : SessionBase<TSession>
{
    private readonly SemaphoreSlim _semaphore;
    private readonly ConcurrentQueue<TSession> _idleSessions = new();
    private readonly int _createSessionTimeoutMs;
    private readonly int _size;

    protected readonly ILogger<SessionPool<TSession>> Logger;

    private volatile int _waitingCount;
    private volatile bool _disposed;

    protected SessionPool(ILogger<SessionPool<TSession>> logger, SessionPoolConfig sessionPoolConfig)
    {
        Logger = logger;
        _size = sessionPoolConfig.MaxSessionPool;
        _createSessionTimeoutMs = sessionPoolConfig.CreateSessionTimeout * 1000;
        _semaphore = new SemaphoreSlim(_size);
    }

    internal async Task<TSession> GetSession(CancellationToken cancellationToken = default)
    {
        using var ctsGetSession = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        if (_createSessionTimeoutMs > 0)
        {
            ctsGetSession.CancelAfter(_createSessionTimeoutMs);
        }

        var finalCancellationToken = ctsGetSession.Token;

        Interlocked.Increment(ref _waitingCount);

        if (_disposed)
        {
            throw new YdbException("Session pool is closed");
        }

        await _semaphore.WaitAsync(finalCancellationToken);
        Interlocked.Decrement(ref _waitingCount);

        if (_idleSessions.TryDequeue(out var session) && session.IsActive)
        {
            return session;
        }

        if (session != null) // not active
        {
            Logger.LogDebug("Session[{Id}] isn't active, creating new session", session.SessionId);
        }

        try
        {
            return await CreateSession(finalCancellationToken);
        }
        catch (Exception e)
        {
            Release();

            Logger.LogError(e, "Failed to create a session");
            throw;
        }
    }

    protected abstract Task<TSession> CreateSession(CancellationToken cancellationToken = default);

    // TODO Retry policy and may be move to SessionPool method
    internal async Task<T> ExecOnSession<T>(Func<TSession, Task<T>> onSession, RetrySettings? retrySettings = null)
    {
        retrySettings ??= RetrySettings.DefaultInstance;
        TSession? session = null;

        for (uint attempt = 0; attempt < retrySettings.MaxAttempts; attempt++)
        {
            try
            {
                session = await GetSession();

                return await onSession(session);
            }
            catch (Exception e)
            {
                var statusErr = e switch
                {
                    Driver.TransportException transportException => transportException.Status,
                    StatusUnsuccessfulException unsuccessfulException => unsuccessfulException.Status,
                    _ => null
                };

                if (attempt == retrySettings.MaxAttempts - 1)
                {
                    if (statusErr != null)
                    {
                        session?.OnStatus(statusErr);
                    }

                    throw;
                }

                if (statusErr != null)
                {
                    session?.OnStatus(statusErr);
                    var retryRule = retrySettings.GetRetryRule(statusErr.StatusCode);

                    if (retryRule.Policy == RetryPolicy.None ||
                        (retryRule.Policy == RetryPolicy.IdempotentOnly && !retrySettings.IsIdempotent))
                    {
                        throw;
                    }

                    Logger.LogTrace(
                        "Retry: attempt {attempt}, Session ${session.SessionId}, idempotent error {status} retrying",
                        attempt, session?.SessionId, statusErr);


                    await Task.Delay(retryRule.BackoffSettings.CalcBackoff(attempt));
                }
                else
                {
                    throw;
                }
            }
            finally
            {
                if (session != null)
                {
                    await session.Release();
                }
            }
        }

        throw new InvalidOperationException("MaxAttempts less then 1, actual value: " + retrySettings.MaxAttempts);
    }

    internal async ValueTask ReleaseSession(TSession session)
    {
        try
        {
            if (_disposed)
            {
                await DeleteSession(session);
                await TryDriverDispose(_size - 1);

                return;
            }

            if (session.IsActive)
            {
                _idleSessions.Enqueue(session);
            }
            else
            {
                _ = DeleteSession(session);
            }
        }
        finally
        {
            Release();
        }
    }

    private void Release() => _semaphore.Release();

    private Task DeleteSession(TSession session) =>
        session.DeleteSession().ContinueWith(s =>
        {
            Logger.LogDebug("Session[{id}] removed with status {status}", session.SessionId, s.Result);
        });

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        var tasks = new List<Task>();
        while (_idleSessions.TryDequeue(out var session)) // thread safe iteration
        {
            tasks.Add(DeleteSession(session));
        }

        await Task.WhenAll(tasks);
        await TryDriverDispose(_size);
    }

    protected virtual ValueTask DisposeDriver() => default;

    private async ValueTask TryDriverDispose(int expectedCurrentCount)
    {
        if (_disposed && _waitingCount == 0 && _semaphore.CurrentCount == expectedCurrentCount)
        {
            await DisposeDriver();
        }
    }
}

public abstract class SessionBase<T> where T : SessionBase<T>
{
    private readonly SessionPool<T> _sessionPool;
    private readonly ILogger _logger;
    private readonly long _nodeId;

    public string SessionId { get; }

    internal volatile bool IsActive = true;

    internal SessionBase(SessionPool<T> sessionPool, string sessionId, long nodeId, ILogger logger)
    {
        _sessionPool = sessionPool;
        SessionId = sessionId;
        _nodeId = nodeId;
        _logger = logger;
    }

    internal void OnStatus(Status status)
    {
        // ReSharper disable once InvertIf
        if (status.StatusCode is
            StatusCode.Cancelled or
            StatusCode.BadSession or
            StatusCode.SessionBusy or
            StatusCode.InternalError or
            StatusCode.ClientTransportTimeout or
            StatusCode.Unavailable or
            StatusCode.ClientTransportUnavailable)
        {
            _logger.LogWarning("Session[{SessionId}] is deactivated. Reason: {Status}", SessionId, status);

            IsActive = false;
        }
    }

    internal ValueTask Release() => _sessionPool.ReleaseSession((T)this);

    internal TS MakeGrpcRequestSettings<TS>(TS settings) where TS : GrpcRequestSettings
    {
        settings.NodeId = _nodeId;
        return settings;
    }

    internal abstract Task<Status> DeleteSession();
}

internal record SessionPoolConfig(
    int MaxSessionPool = SessionPoolDefaultSettings.MaxSessionPool,
    int CreateSessionTimeout = SessionPoolDefaultSettings.CreateSessionTimeoutSeconds,
    bool DisposeDriver = false
);

internal static class SessionPoolDefaultSettings
{
    internal const int MaxSessionPool = 100;

    internal const int CreateSessionTimeoutSeconds = 5;
}
