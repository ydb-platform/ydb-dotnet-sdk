using Microsoft.Extensions.Logging;
using Ydb.Sdk.Client;

namespace Ydb.Sdk.Services.Shared;

public class SessionPoolConfig
{
    public SessionPoolConfig(
        uint? sizeLimit = null)
    {
        SizeLimit = sizeLimit ?? 100;
    }

    public uint SizeLimit { get; }

    public TimeSpan KeepAliveIdleThreshold { get; set; } = TimeSpan.FromMinutes(5);
    public TimeSpan PeriodicCheckInterval { get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan KeepAliveTimeout { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan CreateSessionTimeout { get; set; } = TimeSpan.FromSeconds(1);
}

public class GetSessionResponse<TSession> : ResponseWithResultBase<TSession>, IDisposable where TSession : Session
{
    internal GetSessionResponse(Status status, TSession? session = null)
        : base(status, session)
    {
    }

    public void Dispose()
    {
        Dispose(true);
    }

    protected void Dispose(bool disposing)
    {
        if (disposing)
        {
            Result.Dispose();
        }
    }
}

internal interface ISessionPool<TSession> : IDisposable where TSession : Session
{
    public Task<GetSessionResponse<TSession>> GetSession();
}

internal class NoPool<TSession> : ISessionPool<TSession> where TSession : Session
{
    public Task<GetSessionResponse<TSession>> GetSession()
    {
        throw new InvalidOperationException("Unexpected session pool access.");
    }

    public void Dispose()
    {
    }
}

public abstract class SessionPool<TSession, TClient> : ISessionPool<TSession>
    where TSession : Session
    where TClient : ClientBase
{
    private protected readonly Driver Driver;
    private protected readonly TClient Client;
    private protected readonly ILogger Logger;
    private protected readonly SessionPoolConfig Config;


    protected readonly object Lock = new();
    protected bool Disposed;


    private protected readonly Dictionary<string, SessionState> Sessions = new();
    private protected readonly Stack<string> IdleSessions = new();
    protected uint PendingSessions;

    protected SessionPool(Driver driver, SessionPoolConfig config, TClient client, ILogger logger)
    {
        Driver = driver;
        Config = config;
        Client = client;
        Logger = logger;
    }

    // public virtual Task<GetSessionResponse<TSession>> GetSession()
    // {
    //     throw new NotImplementedException();
    // }

    public async Task<GetSessionResponse<TSession>> GetSession()
    {
        const int maxAttempts = 100;

        GetSessionResponse<TSession> getSessionResponse = null!;
        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            getSessionResponse = await AttemptGetSession();
            if (getSessionResponse.Status.IsSuccess) return getSessionResponse;
        }

        Logger.LogError($"Failed to get session from pool or create it (attempts: {maxAttempts})");
        return getSessionResponse;
    }

    private async Task<GetSessionResponse<TSession>> AttemptGetSession()
    {
        lock (Lock)
        {
            while (IdleSessions.Count > 0)
            {
                var sessionId = IdleSessions.Pop();

                if (!Sessions.TryGetValue(sessionId, out var sessionState))
                {
                    continue;
                }

                Logger.LogTrace($"Session removed from pool: {sessionId}");
                return new GetSessionResponse<TSession>(new Status(StatusCode.Success), sessionState.Session);
            }

            if (Sessions.Count + PendingSessions >= Config.SizeLimit)
            {
                Logger.LogWarning($"Session pool size limit exceeded" +
                                  $", limit: {Config.SizeLimit}" +
                                  $", pending sessions: {PendingSessions}");

                var status = new Status(StatusCode.ClientResourceExhausted, new List<Issue>
                {
                    new("Session pool max active sessions limit exceeded.")
                });

                return new GetSessionResponse<TSession>(status);
            }

            ++PendingSessions;
        }

        return await CreateSession();
    }

    public void ReturnSession(string id)
    {
        lock (Lock)
        {
            if (!Sessions.TryGetValue(id, out var oldSession))
            {
                return;
            }

            var session = CopySession(oldSession.Session);

            Sessions[id] = new SessionState(session);
            IdleSessions.Push(id);

            Logger.LogTrace($"Session returned to pool: {session.Id}");
        }
    }

    private protected abstract Task<GetSessionResponse<TSession>> CreateSession();
    private protected abstract TSession CopySession(TSession other);
    private protected abstract void DeleteSession(string id);

    internal void InvalidateSession(string id)
    {
        lock (Lock)
        {
            Sessions.Remove(id);
            Logger.LogInformation($"Session invalidated in pool: {id}");
        }
    }

    public void Dispose()
    {
        Dispose(true);
    }

    private void Dispose(bool disposing)
    {
        lock (Lock)
        {
            if (Disposed)
            {
                return;
            }

            if (disposing)
            {
                foreach (var state in Sessions.Values)
                {
                    Logger.LogTrace($"Closing session on session pool dispose: {state.Session.Id}");

                    DeleteSession(state.Session.Id);
                }
            }

            Disposed = true;
        }
    }

    internal async Task<IResponse> ExecOnSession(
        Func<TSession, Task<IResponse>> func,
        RetrySettings? retrySettings)
    {
        retrySettings ??= new RetrySettings();

        IResponse response = new ClientInternalErrorResponse("SessionRetry, unexpected response value.");
        TSession? session = null;

        try
        {
            for (uint attempt = 0; attempt < retrySettings.MaxAttempts; attempt++)
            {
                if (session is null)
                {
                    var getSessionResponse = await GetSession();
                    if (getSessionResponse.Status.IsSuccess)
                    {
                        session = getSessionResponse.Result;
                    }

                    response = getSessionResponse;
                }

                if (session is not null)
                {
                    var funcResponse = await func(session);
                    if (funcResponse.Status.IsSuccess)
                    {
                        ReturnSession(session.Id);
                        session = null;
                        return funcResponse;
                    }

                    response = funcResponse;
                }

                var retryRule = retrySettings.GetRetryRule(response.Status.StatusCode);
                if (session is not null)
                {
                    if (retryRule.DeleteSession)
                    {
                        Logger.LogTrace($"Retry: Session ${session.Id} invalid, disposing");
                        InvalidateSession(session.Id);
                    }
                    else
                    {
                        ReturnSession(session.Id);
                    }
                }

                if (retryRule.Idempotency == Idempotency.Idempotent && retrySettings.IsIdempotent ||
                    retryRule.Idempotency == Idempotency.NonIdempotent)
                {
                    Logger.LogTrace(
                        $"Retry: Session ${session?.Id}, " +
                        $"idempotent error {response.Status.StatusCode} retrying ");
                    await Task.Delay(retryRule.BackoffSettings.CalcBackoff(attempt));
                }
                else
                {
                    return response;
                }
            }
        }
        finally
        {
            session?.Dispose();
        }

        return response;
    }

    protected class SessionState
    {
        public SessionState(TSession session)
        {
            Session = session;
            LastAccessTime = DateTime.Now;
        }

        public TSession Session { get; }
        public DateTime LastAccessTime { get; set; }
    }
}