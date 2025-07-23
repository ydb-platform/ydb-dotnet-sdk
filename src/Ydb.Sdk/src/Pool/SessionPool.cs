using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Ado;
using Exception = System.Exception;

namespace Ydb.Sdk.Pool;

internal abstract class SessionPool<TSession> where TSession : SessionBase<TSession>
{
    private readonly SemaphoreSlim _semaphore;
    private readonly ConcurrentQueue<TSession> _idleSessions = new();
    private readonly int _createSessionTimeout;
    private readonly int _size;

    protected readonly SessionPoolConfig Config;
    protected readonly ILogger<SessionPool<TSession>> Logger;

    private volatile int _waitingCount;
    private volatile bool _disposed;

    protected SessionPool(ILogger<SessionPool<TSession>> logger, SessionPoolConfig config)
    {
        Logger = logger;
        Config = config;
        _size = config.MaxSessionPool;
        _createSessionTimeout = config.CreateSessionTimeout;
        _semaphore = new SemaphoreSlim(_size);
    }

    internal async Task<TSession> GetSession(CancellationToken cancellationToken = default)
    {
        try
        {
            using var ctsGetSession = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            if (_createSessionTimeout > 0)
            {
                ctsGetSession.CancelAfter(TimeSpan.FromSeconds(_createSessionTimeout));
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

                _ = DeleteSession(session);
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
        catch (OperationCanceledException e)
        {
            throw new YdbException(StatusCode.Cancelled,
                $"The connection pool has been exhausted, either raise 'MaxSessionPool' " +
                $"(currently {_size}) or 'CreateSessionTimeout' " +
                $"(currently {_createSessionTimeout} seconds) in your connection string.", e
            );
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
            catch (YdbException e)
            {
                if (attempt == retrySettings.MaxAttempts - 1)
                {
                    session?.OnNotSuccessStatusCode(e.Code);

                    throw;
                }

                session?.OnNotSuccessStatusCode(e.Code);
                var retryRule = retrySettings.GetRetryRule(e.Code);

                if (retryRule.Policy == RetryPolicy.None ||
                    (retryRule.Policy == RetryPolicy.IdempotentOnly && !retrySettings.IsIdempotent))
                {
                    throw;
                }

                Logger.LogTrace(e, "Retry: attempt {attempt}, Session ${session.SessionId}, idempotent error retrying",
                    attempt, session?.SessionId);


                await Task.Delay(retryRule.BackoffSettings.CalcBackoff(attempt));
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

    private async Task DeleteSession(TSession session)
    {
        try
        {
            await session.DeleteSession();
        }
        catch (YdbException e)
        {
            Logger.LogError(e, "Failed to delete session");
        }
    }

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

    internal void OnNotSuccessStatusCode(StatusCode code)
    {
        // ReSharper disable once InvertIf
        if (code is
            StatusCode.BadSession or
            StatusCode.SessionBusy or
            StatusCode.SessionExpired or
            StatusCode.ClientTransportTimeout or
            StatusCode.ClientTransportUnavailable)
        {
            _logger.LogWarning("Session[{SessionId}] is deactivated. Reason Status: {Status}", SessionId, code);

            IsActive = false;
        }
    }

    internal ValueTask Release() => _sessionPool.ReleaseSession((T)this);

    internal TS MakeGrpcRequestSettings<TS>(TS settings) where TS : GrpcRequestSettings
    {
        settings.NodeId = _nodeId;
        return settings;
    }

    internal abstract Task DeleteSession();
}

internal record SessionPoolConfig(
    int MaxSessionPool = SessionPoolDefaultSettings.MaxSessionPool,
    int CreateSessionTimeout = SessionPoolDefaultSettings.CreateSessionTimeoutSeconds,
    bool DisposeDriver = false,
    bool DisableServerBalancer = SessionPoolDefaultSettings.DisableServerBalancer
);

internal static class SessionPoolDefaultSettings
{
    internal const int MaxSessionPool = 100;

    internal const int CreateSessionTimeoutSeconds = 5;

    internal const bool DisableServerBalancer = false;
}
