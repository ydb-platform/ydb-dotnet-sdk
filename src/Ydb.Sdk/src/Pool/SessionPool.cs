using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Ado;
using Exception = System.Exception;

namespace Ydb.Sdk.Pool;

internal abstract class SessionPool<TSession> where TSession : SessionBase<TSession>
{
    private readonly ILogger<SessionPool<TSession>> _logger;
    private readonly SemaphoreSlim _semaphore;
    private readonly ConcurrentQueue<TSession> _idleSessions = new();
    private readonly int _size;

    private volatile int _waitingCount;
    private volatile bool _disposed;

    protected SessionPool(ILogger<SessionPool<TSession>> logger, int? maxSessionPool = null)
    {
        _logger = logger;
        _size = maxSessionPool ?? 100;
        _semaphore = new SemaphoreSlim(_size);
    }

    internal async Task<TSession> GetSession()
    {
        Interlocked.Increment(ref _waitingCount);

        if (_disposed)
        {
            throw new YdbException("Session pool is closed");
        }

        await _semaphore.WaitAsync();
        Interlocked.Decrement(ref _waitingCount);

        if (_idleSessions.TryDequeue(out var session) && session.IsActive)
        {
            return session;
        }

        if (session != null) // not active
        {
            _ = DeleteNotActiveSession(session);
        }

        try
        {
            return await CreateSession();
        }
        catch (Driver.TransportException) // Transport exception
        {
            Release();

            throw;
        }
        catch (StatusUnsuccessfulException)
        {
            Release();

            throw;
        }
    }

    protected abstract Task<TSession> CreateSession();

    protected abstract Task<Status> DeleteSession(TSession session);

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
                if (attempt == retrySettings.MaxAttempts - 1)
                {
                    throw;
                }

                var statusErr = e switch
                {
                    Driver.TransportException transportException => transportException.Status,
                    StatusUnsuccessfulException unsuccessfulException => unsuccessfulException.Status,
                    _ => null
                };

                if (statusErr != null)
                {
                    var retryRule = retrySettings.GetRetryRule(statusErr.StatusCode);

                    if (retryRule.Policy == RetryPolicy.None ||
                        (retryRule.Policy == RetryPolicy.IdempotentOnly && !retrySettings.IsIdempotent))
                    {
                        throw;
                    }

                    _logger.LogTrace(
                        "Retry: attempt {attempt}, Session ${session?.SessionId}, idempotent error {status} retrying",
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
                await DeleteNotActiveSession(session);
                await TryDriverDispose(_size - 1);

                return;
            }

            if (session.IsActive)
            {
                _idleSessions.Enqueue(session);
            }
            else
            {
                _ = DeleteNotActiveSession(session);
            }
        }
        finally
        {
            Release();
        }
    }

    private void Release()
    {
        _semaphore.Release();
    }

    private Task DeleteNotActiveSession(TSession session)
    {
        return DeleteSession(session).ContinueWith(s =>
        {
            _logger.LogDebug("Session[{id}] removed with status {status}", session.SessionId, s.Result);
        });
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
            tasks.Add(DeleteNotActiveSession(session));
        }

        await Task.WhenAll(tasks);
        await TryDriverDispose(_size);
    }

    protected virtual ValueTask DisposeDriver()
    {
        return default;
    }

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

    internal ValueTask Release()
    {
        return _sessionPool.ReleaseSession((T)this);
    }
}
