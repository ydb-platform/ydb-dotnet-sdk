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