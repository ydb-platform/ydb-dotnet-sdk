using Microsoft.Extensions.Logging;
using Ydb.Query;
using Ydb.Query.V1;
using Ydb.Sdk.Pool;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query;

internal sealed class SessionPool : SessionPool<Session>, IAsyncDisposable
{
    private static readonly CreateSessionRequest CreateSessionRequest = new();

    private static readonly GrpcRequestSettings CreateSessionSettings = new()
    {
        TransportTimeout = TimeSpan.FromMinutes(2)
    };

    private static readonly GrpcRequestSettings AttachSessionSettings = new()
    {
        TransportTimeout = TimeSpan.FromMinutes(1)
    };

    private readonly Driver _driver;
    private readonly bool _disposingDriver;

    internal SessionPool(Driver driver, int? maxSessionPool = null, bool disposingDriver = false)
        : base(driver.LoggerFactory.CreateLogger<SessionPool>(), maxSessionPool)
    {
        _driver = driver;
        _disposingDriver = disposingDriver;
    }

    protected override async Task<Session> CreateSession()
    {
        var response = await _driver.UnaryCall(QueryService.CreateSessionMethod, CreateSessionRequest,
            CreateSessionSettings);

        var status = Status.FromProto(response.Status, response.Issues);

        status.EnsureSuccess();

        TaskCompletionSource<Status> completeTask = new();

        var session = new Session(_driver, this, response.SessionId, response.NodeId);

        _ = Task.Run(async () =>
        {
            try
            {
                await using var stream = _driver.ServerStreamCall(QueryService.AttachSessionMethod,
                    new AttachSessionRequest { SessionId = session.SessionId }, AttachSessionSettings);

                if (!await stream.MoveNextAsync())
                {
                    // Session wasn't started!
                    completeTask.SetResult(new Status(StatusCode.Cancelled, "Attach stream is not started!"));

                    return;
                }

                completeTask.SetResult(Status.FromProto(stream.Current.Status, stream.Current.Issues));

                try
                {
                    await foreach (var sessionState in stream) // watch attach stream session cycle life
                    {
                        var sessionStateStatus = Status.FromProto(sessionState.Status, sessionState.Issues);

                        Logger.LogDebug("Session[{SessionId}] was received the status from the attach stream: {Status}",
                            session.SessionId, sessionStateStatus);

                        session.OnStatus(sessionStateStatus);

                        // ReSharper disable once InvertIf
                        if (!session.IsActive)
                        {
                            Logger.LogWarning("Session[{SessionId}] is deactivated. Reason: {Status}",
                                session.SessionId, sessionStateStatus);

                            return;
                        }
                    }

                    Logger.LogDebug("Session[{SessionId}]: Attached stream is closed", session.SessionId);

                    // attach stream is closed
                }
                catch (Driver.TransportException e)
                {
                    Logger.LogWarning(e, "Session[{SessionId}] is deactivated by transport error", session.SessionId);
                }
            }
            catch (Driver.TransportException e)
            {
                Logger.LogError(e, "Transport error on attach session");

                completeTask.SetException(e);
            }
            finally
            {
                session.IsActive = false;
            }
        });

        (await completeTask.Task).EnsureSuccess();

        return session;
    }

    protected override async Task<Status> DeleteSession(Session session)
    {
        try
        {
            var settings = new GrpcRequestSettings
            {
                TransportTimeout = TimeSpan.FromSeconds(5),
                NodeId = session.NodeId
            };

            var deleteSessionResponse = await _driver.UnaryCall(QueryService.DeleteSessionMethod,
                new DeleteSessionRequest { SessionId = session.SessionId }, settings);

            return Status.FromProto(deleteSessionResponse.Status, deleteSessionResponse.Issues);
        }
        catch (Driver.TransportException e)
        {
            return e.Status;
        }
    }

    protected override ValueTask DisposeDriver()
    {
        return _disposingDriver ? _driver.DisposeAsync() : default;
    }
}

internal class Session : SessionBase<Session>
{
    private readonly Driver _driver;

    internal Session(Driver driver, SessionPool<Session> sessionPool, string sessionId, long nodeId)
        : base(sessionPool, sessionId, nodeId)
    {
        _driver = driver;
    }

    internal ServerStream<ExecuteQueryResponsePart> ExecuteQuery(
        string query,
        Dictionary<string, YdbValue>? parameters,
        ExecuteQuerySettings? settings,
        TransactionControl? txControl)
    {
        parameters ??= new Dictionary<string, YdbValue>();
        settings ??= ExecuteQuerySettings.DefaultInstance;
        settings.NodeId = NodeId;

        var request = new ExecuteQueryRequest
        {
            SessionId = SessionId,
            ExecMode = ExecMode.Execute,
            QueryContent = new QueryContent { Text = query, Syntax = (Ydb.Query.Syntax)settings.Syntax },
            StatsMode = StatsMode.None,
            TxControl = txControl
        };

        request.Parameters.Add(parameters.ToDictionary(p => p.Key, p => p.Value.GetProto()));

        return _driver.ServerStreamCall(QueryService.ExecuteQueryMethod, request, settings);
    }

    internal async Task<Status> CommitTransaction(string txId, GrpcRequestSettings? settings = null)
    {
        settings ??= GrpcRequestSettings.DefaultInstance;
        settings.NodeId = NodeId;

        var response = await _driver.UnaryCall(QueryService.CommitTransactionMethod, new CommitTransactionRequest
            { SessionId = SessionId, TxId = txId }, settings);

        var status = Status.FromProto(response.Status, response.Issues);

        OnStatus(status);

        return status;
    }

    internal async Task<Status> RollbackTransaction(string txId, GrpcRequestSettings? settings = null)
    {
        settings ??= GrpcRequestSettings.DefaultInstance;
        settings.NodeId = NodeId;

        var response = await _driver.UnaryCall(QueryService.RollbackTransactionMethod, new RollbackTransactionRequest
            { SessionId = SessionId, TxId = txId }, settings);

        var status = Status.FromProto(response.Status, response.Issues);

        OnStatus(status);

        return status;
    }
}
