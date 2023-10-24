using Microsoft.Extensions.Logging;
using Ydb.Sdk.Client;

namespace Ydb.Sdk.Services.Query;

public class SessionPoolConfig
{
    public SessionPoolConfig(
        uint sizeLimit = 100)
    {
        SizeLimit = sizeLimit;
    }

    public readonly uint SizeLimit;
    public TimeSpan CreateSessionTimeout { get; set; } = TimeSpan.FromSeconds(1);
    public TimeSpan DeleteSessionTimeout { get; set; } = TimeSpan.FromSeconds(1);
}

internal class GetSessionResponse : ResponseBase
{
    public Session? Session;

    internal GetSessionResponse(Status status, Session? session = null)
        : base(status)
    {
        Session = session;
    }
}

internal interface ISessionPool : IDisposable
{
}

internal class NoPool : ISessionPool
{
    public void Dispose()
    {
    }
}

internal class SessionPool : ISessionPool
{
    private readonly Driver _driver;
    private readonly SessionPoolConfig _config;

    private readonly object _lock = new();

    private readonly ILogger _logger;
    private readonly QueryClient _client;

    private readonly Dictionary<string, Session> _sessions = new();
    private readonly Stack<string> _idleSessions = new();
    private uint _pendingSessions;

    private bool _disposed;

    public SessionPool(Driver driver, SessionPoolConfig config)
    {
        _driver = driver;
        _config = config;

        _logger = driver.LoggerFactory.CreateLogger<SessionPool>();
        _client = new QueryClient(driver, new NoPool());
    }

    public async Task<GetSessionResponse> GetSession()
    {
        var response = new GetSessionResponse(new Status(StatusCode.ClientInternalError,
            $"{nameof(SessionPool)},{nameof(GetSession)}: unexpected response value."));

        const int maxAttempts = 100;
        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            lock (_lock)
            {
                while (_idleSessions.Count > 0)
                {
                    var sessionId = _idleSessions.Pop();

                    if (_sessions.TryGetValue(sessionId, out var session))
                    {
                        _logger.LogTrace(
                            $"{nameof(SessionPool)},{nameof(GetSession)}: Session {sessionId} removed from pool");
                        return new GetSessionResponse(Status.Success, session);
                    }
                }

                if (_sessions.Count + _pendingSessions >= _config.SizeLimit)
                {
                    _logger.LogWarning($"{nameof(SessionPool)},{nameof(GetSession)}: size limit exceeded" +
                                       $", limit: {_config.SizeLimit}" +
                                       $", pending sessions: {_pendingSessions}");

                    return new GetSessionResponse(new Status(StatusCode.ClientResourceExhausted,
                        $"{nameof(SessionPool)},{nameof(GetSession)}: max active sessions limit exceeded."));
                }

                _pendingSessions++;
            }

            var createSessionResponse = await _client.CreateSession(new CreateSessionSettings
                { TransportTimeout = _config.CreateSessionTimeout });

            lock (_lock)
            {
                _pendingSessions--;
                if (createSessionResponse.Status.IsSuccess)
                {
                    var session = createSessionResponse.Session!;
                    session.SessionPool = this;

                    _sessions.Add(session.Id, session);
                    _logger.LogTrace($"Session {session.Id} created, " +
                                     $"endpoint: {session.Endpoint}, " +
                                     $"nodeId: {session.NodeId}");
                    return new GetSessionResponse(createSessionResponse.Status, session);
                }

                _logger.LogWarning($"Failed to create session: {createSessionResponse.Status}");
                response = new GetSessionResponse(createSessionResponse.Status);
            }
        }

        return response;
    }


    public void ReturnSession(Session session)
    {
        _idleSessions.Push(session.Id);
        _sessions[session.Id] = session;
        _logger.LogTrace($"Session returned to pool: {session.Id}");
    }

    internal void DisposeSession(Session session)
    {
        _sessions.Remove(session.Id);
    }

    public void Dispose()
    {
        Dispose(true);
    }

    private void Dispose(bool disposing)
    {
        lock (_lock)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                foreach (var session in _sessions.Values)
                {
                    session.Dispose(_config.DeleteSessionTimeout);
                    _logger.LogTrace($"Closing session on session pool dispose: {session.Id}");
                }
            }

            _disposed = true;
        }
    }
}