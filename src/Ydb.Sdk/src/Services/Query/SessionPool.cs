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


                _ = Task.Run(() => AttachAndMonitor(session.Id));

                Sessions.Add(session.Id, new SessionState(session));

                Logger.LogTrace($"Session {session.Id} created, " +
                                $"endpoint: {session.Endpoint}, " +
                                $"nodeId: {session.NodeId}");
                return new GetSessionResponse(createSessionResponse.Status, session);
            }

            Logger.LogWarning($"Failed to create session: {createSessionResponse.Status}");
            return new GetSessionResponse(createSessionResponse.Status);
        }
    }

    private async Task AttachAndMonitor(string id)
    {
        var stream = Client.AttachSession(id);

        while (await stream.Next())
        {
            var part = stream.Response;

            if (part.Status.IsSuccess)
            {
                Logger.LogTrace($"Successful stream response for session: {id}");

                lock (Lock)
                {
                    if (Sessions.TryGetValue(id, out var sessionState))
                    {
                        sessionState.LastAccessTime = DateTime.Now;
                    }
                }
            }
            else
            {
                InvalidateSession(id);
            }
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