using Microsoft.Extensions.Logging;
using Ydb.Sdk.Services.Sessions;

namespace Ydb.Sdk.Services.Table;

using GetSessionResponse = GetSessionResponse<Session>;
using NoPool = NoPool<Session>;

internal sealed class SessionPool : SessionPoolBase<Session>
{
    private readonly TableClient _tableClient;

    public SessionPool(IDriver driver, SessionPoolConfig config) :
        base(driver: driver, config: config, logger: driver.LoggerFactory.CreateLogger<SessionPool>())
    {
        _tableClient = new TableClient(driver, new NoPool());

        Task.Run(PeriodicCheck);
    }

    private protected override async Task<GetSessionResponse> CreateSession()
    {
        var createSessionResponse = await _tableClient.CreateSession(new CreateSessionSettings
        {
            TransportTimeout = Config.CreateSessionTimeout,
            OperationTimeout = Config.CreateSessionTimeout
        });

        lock (Lock)
        {
            --PendingSessions;

            if (createSessionResponse.Status.IsSuccess)
            {
                var session = new Session(
                    driver: Driver,
                    sessionPool: this,
                    id: createSessionResponse.Result.Session.Id,
                    nodeId: createSessionResponse.Result.Session.NodeId);

                Sessions.Add(session.Id, new SessionState(session));

                Logger.LogTrace($"Session created from pool: {session.Id}");

                return new GetSessionResponse(createSessionResponse.Status, session);
            }

            Logger.LogWarning($"Failed to create session: {createSessionResponse.Status}");
        }

        return new GetSessionResponse(createSessionResponse.Status);
    }

    private protected override Session CopySession(Session other)
    {
        return new Session(
            driver: Driver,
            sessionPool: this,
            id: other.Id,
            nodeId: other.NodeId);
    }

    private async Task PeriodicCheck()
    {
        var stop = false;
        while (!stop)
        {
            try
            {
                await Task.Delay(Config.PeriodicCheckInterval);
                await CheckSessions();
            }
            catch (Exception e)
            {
                Logger.LogError($"Unexpected exception during session pool periodic check: {e}");
            }

            lock (Lock)
            {
                stop = Disposed;
            }
        }
    }

    private async Task CheckSessions()
    {
        Logger.LogDebug("Check sessions" +
                        $", sessions: {Sessions.Count}" +
                        $", pending sessions: {PendingSessions}" +
                        $", idle sessions: {IdleSessions.Count}");

        var keepAliveIds = new List<string>();

        lock (Lock)
        {
            foreach (var state in Sessions.Values)
            {
                if (state.LastAccessTime + Config.KeepAliveIdleThreshold < DateTime.Now)
                {
                    keepAliveIds.Add(state.Session.Id);
                }
            }
        }

        foreach (var id in keepAliveIds)
        {
            var response = await _tableClient.KeepAlive(id, new KeepAliveSettings
            {
                TransportTimeout = Config.KeepAliveTimeout,
                OperationTimeout = Config.KeepAliveTimeout
            });

            if (response.Status.IsSuccess)
            {
                Logger.LogTrace($"Successful keepalive for session: {id}");

                lock (Lock)
                {
                    if (Sessions.TryGetValue(id, out var sessionState))
                    {
                        sessionState.LastAccessTime = DateTime.Now;
                    }
                }
            }
            else if (response.Status.StatusCode == StatusCode.BadSession)
            {
                Logger.LogInformation($"Session invalidated by keepalive: {id}");

                lock (Lock)
                {
                    Sessions.Remove(id);
                }
            }
            else
            {
                Logger.LogWarning("Unsuccessful keepalive" +
                                  $", session: {id}" +
                                  $", status: {response.Status}");
            }
        }
    }

    private protected override async Task DeleteSession(string id)
    {
        await _tableClient.DeleteSession(id, new DeleteSessionSettings
        {
            TransportTimeout = SessionBase.DeleteSessionTimeout
        });
    }
}
