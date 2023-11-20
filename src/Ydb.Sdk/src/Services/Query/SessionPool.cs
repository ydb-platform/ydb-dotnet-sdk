using Microsoft.Extensions.Logging;
using Ydb.Sdk.Services.Sessions;

namespace Ydb.Sdk.Services.Query;

using GetSessionResponse = GetSessionResponse<Session>;
using NoPool = NoPool<Session>;

internal class SessionPool : SessionPoolBase<Session, QueryClient>
{
    private readonly Dictionary<string, CancellationTokenSource> _attachedSessions = new();

    public SessionPool(Driver driver, SessionPoolConfig config) :
        base(
            driver: driver,
            config: config,
            client: new QueryClient(driver, new NoPool()),
            logger: driver.LoggerFactory.CreateLogger<SessionPool>())
    {
    }

    private protected override async Task<GetSessionResponse> CreateSession()
    {
        var createSessionResponse = await Client.CreateSession(new CreateSessionSettings
            { TransportTimeout = Config.CreateSessionTimeout });

        lock (Lock)
        {
            PendingSessions--;
            if (createSessionResponse.Status.IsSuccess)
            {
                var session = new Session(
                    driver: Driver,
                    sessionPool: this,
                    id: createSessionResponse.Result.Session.Id,
                    nodeId: createSessionResponse.Result.Session.NodeId,
                    endpoint: createSessionResponse.Result.Session.Endpoint);

                Sessions.Add(session.Id, new SessionState(session));

                _ = Task.Run(() => AttachAndMonitor(session.Id));


                Logger.LogTrace($"Session {session.Id} created, " +
                                $"endpoint: {session.Endpoint}, " +
                                $"nodeId: {session.NodeId}");
                return new GetSessionResponse(createSessionResponse.Status, session);
            }

            Logger.LogWarning($"Failed to create session: {createSessionResponse.Status}");
            return new GetSessionResponse(createSessionResponse.Status);
        }
    }

    private async Task AttachAndMonitor(string sessionId)
    {
        var stream = Client.AttachSession(sessionId);

        var cts = new CancellationTokenSource();
        cts.CancelAfter(Config.CreateSessionTimeout);

        var firstPartTask = Task.Run(async () =>
        {
            if (await stream.Next())
            {
                return stream.Response;
            }

            return null;
        }, cts.Token);

        var firstPart = await firstPartTask;

        if (firstPartTask.IsCanceled || firstPart is null)
        {
            InvalidateSession(sessionId);
            return;
        }

        CheckPart(firstPart, sessionId);

        cts = new CancellationTokenSource();

        var monitorTask = Task.Run(async () => await Monitor(sessionId, stream), cts.Token);
        lock (Lock)
        {
            _attachedSessions.Add(sessionId, cts);
        }

        await monitorTask;
        lock (Lock)
        {
            _attachedSessions.Remove(sessionId);
        }
    }

    private async Task Monitor(string sessionId, SessionStateStream stream)
    {
        while (await stream.Next())
        {
            var part = stream.Response;
            if (!CheckPart(part, sessionId))
            {
                break;
            }
        }
    }

    private bool CheckPart(Query.SessionState part, string sessionId)
    {
        if (part.Status.IsSuccess)
        {
            Logger.LogTrace($"Successful stream response for session: {sessionId}");

            lock (Lock)
            {
                if (Sessions.TryGetValue(sessionId, out var sessionState))
                {
                    sessionState.LastAccessTime = DateTime.Now;
                }
            }

            return true;
        }

        InvalidateSession(sessionId);
        return false;
    }

    private new void InvalidateSession(string id)
    {
        DetachSession(id);
        base.InvalidateSession(id);
    }

    private void DetachSession(string id)
    {
        lock (Lock)
        {
            _attachedSessions.Remove(id, out var cts);
            cts?.Cancel();
            Logger.LogInformation($"Session detached: {id}");
        }
    }

    private protected override Session CopySession(Session other)
    {
        return new Session(
            driver: Driver,
            sessionPool: this,
            id: other.Id,
            nodeId: other.NodeId,
            endpoint: other.Endpoint);
    }

    private protected override void DeleteSession(string id)
    {
        DetachSession(id);

        _ = Client.DeleteSession(id, new DeleteSessionSettings
        {
            TransportTimeout = SessionBase.DeleteSessionTimeout
        });
    }
}
