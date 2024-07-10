using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Pool;

internal abstract class SessionPool<TSession> where TSession : SessionBase<TSession>
{
    private readonly ILogger<SessionPool<TSession>> _logger;
    private readonly SemaphoreSlim _semaphore;
    private readonly ConcurrentQueue<TSession> _idleSessions = new();
    private readonly int _size;

    private volatile bool _disposed;
    private int _disposedSession;

    protected SessionPool(ILogger<SessionPool<TSession>> logger, int? maxSessionPool = null)
    {
        _logger = logger;
        _size = maxSessionPool ?? 100;
        _semaphore = new SemaphoreSlim(_size);
    }

    internal async Task<(Status, TSession?)> GetSession()
    {
        if (_disposed)
        {
            return (new Status(StatusCode.Cancelled, "Session pool is closed"), null);
        }

        Console.WriteLine("WAIT _semaphore");
        await _semaphore.WaitAsync();
        Console.WriteLine("GET _semaphore");

        if (_idleSessions.TryDequeue(out var session) && session.IsActive)
        {
            return (Status.Success, session);
        }

        if (session != null) // not active
        {
            _ = DeleteNotActiveSession(session);
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

    // TODO Retry policy and may be move to SessionPool method
    internal async Task<T> ExecOnSession<T>(Func<TSession, Task<T>> onSession, RetrySettings? retrySettings = null)
    {
        retrySettings ??= RetrySettings.DefaultInstance;
        var status = new Status(StatusCode.Unspecified);
        TSession? session = null;

        for (uint attempt = 0; attempt < retrySettings.MaxAttempts; attempt++)
        {
            try
            {
                (status, session) = await GetSession();

                status.EnsureSuccess();

                return await onSession(session!);
            }
            catch (Driver.TransportException e)
            {
                status = e.Status;
            }
            catch (StatusUnsuccessfulException e)
            {
                status = e.Status;
            }
            finally
            {
                if (session != null)
                {
                    await session.Release();
                }
            }

            // TODO Retry policy
            var retryRule = retrySettings.GetRetryRule(status.StatusCode);
            // _logger.LogTrace("Retry: attempt {attempt}, Session ${SessionId}, idempotent error {Status} retrying",
            //     attempt, session?.SessionId ?? "was not created", status);

            await Task.Delay(retryRule.BackoffSettings.CalcBackoff(attempt));
        }

        throw new StatusUnsuccessfulException(status);
    }

    internal async ValueTask ReleaseSession(TSession session)
    {
        if (_disposed)
        {
            Console.WriteLine("DELETE SESSION");
            await DeleteNotActiveSession(session);

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

        Console.WriteLine("RELEASE SESSION");
        Release();
    }

    private void Release()
    {
        _semaphore.Release();
    }

    private Task DeleteNotActiveSession(TSession session)
    {
        return DeleteSession(session.SessionId).ContinueWith(async s =>
        {
            _logger.LogDebug("Session[{id}] removed with status {status}", session.SessionId, s.Result);

            if (_size == Interlocked.Increment(ref _disposedSession))
            {
                _logger.LogInformation("Disposing grpc transport");

                await DisposeDriver();
            }
        });
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }
        
        Console.WriteLine("SESSION POOL START CLOSING");

        _disposed = true;

        var tasks = new List<Task>();
        while (_idleSessions.TryDequeue(out var session)) // thread safe iteration
        {
            tasks.Add(DeleteNotActiveSession(session));
        }

        await Task.WhenAll(tasks);
    }

    protected virtual ValueTask DisposeDriver()
    {
        return default;
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
