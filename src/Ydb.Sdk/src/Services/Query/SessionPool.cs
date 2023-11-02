using Microsoft.Extensions.Logging;
using Ydb.Sdk.Services.Shared;

namespace Ydb.Sdk.Services.Query;

using GetSessionResponse = GetSessionResponse<Session>;
using NoPool = NoPool<Session>;

internal class SessionPool : SessionPool<Session, QueryClient>
{
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

        while (await stream.Next())
        {
            var part = stream.Response;
            CheckPart(part, sessionId);
        }
    }

    private void CheckPart(Query.SessionState part, string sessionId)
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
        }
        else
        {
            InvalidateSession(sessionId);
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
        _ = Client.DeleteSession(id, new DeleteSessionSettings
        {
            TransportTimeout = Shared.Session.DeleteSessionTimeout
        });
    }
}