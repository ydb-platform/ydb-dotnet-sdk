using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Ydb.Sdk.Client;

namespace Ydb.Sdk.Table
{
    public class SessionPoolConfig
    {
        public SessionPoolConfig(
            uint? sizeLimit = null)
        {
            SizeLimit = sizeLimit ?? 100;
        }

        public uint SizeLimit { get; }

        internal TimeSpan KeepAliveIdleThreshold { get; } = TimeSpan.FromMinutes(5);
        internal TimeSpan PeriodicCheckInterval { get; } = TimeSpan.FromSeconds(10);
        internal TimeSpan KeepAliveTimeout { get; } = TimeSpan.FromSeconds(1);
        internal TimeSpan CreateSessionTimeout { get; } = TimeSpan.FromSeconds(1);
    }

    internal class GetSessionResponse : ResponseWithResultBase<Session>, IDisposable
    {
        internal GetSessionResponse(Status status, Session? session = null)
            : base(status, session)
        {
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                if (Result != null)
                {
                    Result.Dispose();
                }
            }
        }
    }

    internal interface ISessionPool : IDisposable
    {
        public abstract Task<GetSessionResponse> GetSession();
    }

    internal class NoPool : ISessionPool
    {
        public Task<GetSessionResponse> GetSession()
        {
            throw new InvalidOperationException("Unexpected session pool access.");
        }

        public void Dispose()
        {
        }
    }

    internal sealed class SessionPool : ISessionPool
    {
        private readonly Driver _driver;
        private readonly SessionPoolConfig _config;

        private readonly object _lock = new object();
        private readonly ILogger _logger;
        private readonly TableClient _client;
        private bool _disposed = false;

        private Dictionary<string, SessionState> _sessions = new Dictionary<string, SessionState>();
        private Stack<string> _idleSessions = new Stack<string>();
        private uint _pendingSessions = 0;

        public SessionPool(Driver driver, SessionPoolConfig config)
        {
            _driver = driver;
            _config = config;

            _logger = _driver.LoggerFactory.CreateLogger<SessionPool>();
            _client = new TableClient(_driver, new NoPool());

            Task.Run(PeriodicCheck);
        }

        public async Task<GetSessionResponse> GetSession()
        {
            lock (_lock)
            {
                while (_idleSessions.Count > 0)
                {
                    var sessionId = _idleSessions.Pop();

                    SessionState? sessionState;
                    if (!_sessions.TryGetValue(sessionId, out sessionState))
                    {
                        continue;
                    }

                    _logger.LogTrace($"Session removed from pool: {sessionId}");
                    return new GetSessionResponse(new Status(StatusCode.Success), sessionState.Session);
                }

                if (_sessions.Count + _pendingSessions >= _config.SizeLimit)
                {
                    _logger.LogWarning($"Session pool size limit exceeded" +
                        $", limit: {_config.SizeLimit}" +
                        $", pending sessions: {_pendingSessions}");

                    var status = new Status(StatusCode.ClientResourceExhausted, new List<Issue> {
                        new Issue("Session pool max active sessions limit exceeded.")
                    });

                    return new GetSessionResponse(status);
                }

                ++_pendingSessions;
            }

            var createSessionResponse = await _client.CreateSession(new CreateSessionSettings
            {
                TransportTimeout = _config.CreateSessionTimeout,
                OperationTimeout = _config.CreateSessionTimeout
            });

            lock (_lock)
            {
                --_pendingSessions;

                if (createSessionResponse.Status.IsSuccess)
                {
                    var session = new Session(
                        driver: _driver,
                        sessionPool: this,
                        id: createSessionResponse.Result.Session.Id,
                        endpoint: createSessionResponse.Result.Session.Endpoint);

                    _sessions.Add(session.Id, new SessionState(session));

                    _logger.LogTrace($"Session created from pool: {session.Id}, endpoint: {session.Endpoint}");

                    return new GetSessionResponse(createSessionResponse.Status, session);
                }

                _logger.LogWarning($"Failed to create session: {createSessionResponse.Status}");
            }

            return new GetSessionResponse(createSessionResponse.Status);
        }

        internal void ReturnSession(string id)
        {
            lock (_lock)
            {
                SessionState? oldSession;
                if (_sessions.TryGetValue(id, out oldSession))
                {
                    var session = new Session(
                        driver: _driver,
                        sessionPool: this,
                        id: id,
                        endpoint: oldSession.Session.Endpoint);

                    _sessions[id] = new SessionState(session);
                    _idleSessions.Push(id);

                    _logger.LogTrace($"Session returned to pool: {session.Id}");
                }
            }
        }

        internal void InvalidateSession(string id)
        {
            lock (_lock)
            {
                _sessions.Remove(id);
                _logger.LogInformation($"Session invalidated in pool: {id}");
            }
        }

        private async Task PeriodicCheck()
        {
            bool stop = false;
            while (!stop)
            {
                try
                {
                    await Task.Delay(_config.PeriodicCheckInterval);
                    await CheckSessions();
                }
                catch (Exception e)
                {
                    _logger.LogError($"Unexpected exception during session pool periodic check: {e}");
                }

                lock (_lock)
                {
                    stop = _disposed;
                }
            }
        }

        private async Task CheckSessions()
        {
            _logger.LogDebug($"Check sessions" +
                $", sessions: {_sessions.Count}" +
                $", pending sessions: {_pendingSessions}" +
                $", idle sessions: {_idleSessions.Count}");

            List<string> keepAliveIds = new List<string>();

            lock (_lock)
            {
                foreach (var state in _sessions.Values)
                {
                    if (state.LastAccessTime + _config.KeepAliveIdleThreshold < DateTime.Now)
                    {
                        keepAliveIds.Add(state.Session.Id);
                    }
                }
            }

            foreach (string id in keepAliveIds)
            {
                var response = await _client.KeepAlive(id, new KeepAliveSettings
                {
                    TransportTimeout = _config.KeepAliveTimeout,
                    OperationTimeout = _config.KeepAliveTimeout,
                });

                if (response.Status.IsSuccess)
                {
                    _logger.LogTrace($"Successful keepalive for session: {id}");

                    lock (_lock)
                    {
                        SessionState? sessionState;
                        if (_sessions.TryGetValue(id, out sessionState))
                        {
                            sessionState.LastAccessTime = DateTime.Now;
                        }
                    }
                }
                else if (response.Status.StatusCode == StatusCode.BadSession)
                {
                    _logger.LogInformation($"Session invalidated by keepalive: {id}");

                    lock (_lock)
                    {
                        _sessions.Remove(id);
                    }
                } else
                {
                    _logger.LogWarning($"Unsuccessful keepalive" +
                        $", session: {id}" +
                        $", status: {response.Status}");
                }
            }
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
                    foreach (var state in _sessions.Values) {
                        _logger.LogTrace($"Closing session on session pool dispose: {state.Session.Id}");

                        _ = _client.DeleteSession(state.Session.Id, new DeleteSessionSettings
                        {
                            TransportTimeout = Session.DeleteSessionTimeout
                        });
                    }
                }

                _disposed = true;
            }
        }

        private class SessionState
        {
            public SessionState(Session session)
            {
                Session = session;
                LastAccessTime = DateTime.Now;
            }

            public Session Session { get; }
            public DateTime LastAccessTime { get; set; }

        }
    }
}
