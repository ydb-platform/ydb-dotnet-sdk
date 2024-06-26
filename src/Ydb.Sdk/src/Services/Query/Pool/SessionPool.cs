using Microsoft.Extensions.Logging;
using Ydb.Query;
using Ydb.Query.V1;
using Ydb.Sdk.Pool;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query.Pool;

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

    private static readonly GrpcRequestSettings DeleteSessionSettings = new()
    {
        TransportTimeout = TimeSpan.FromSeconds(5)
    };

    private readonly Driver _driver;

    internal SessionPool(Driver driver, int? size = null) : base(driver.LoggerFactory.CreateLogger<SessionPool>(), size)
    {
        _driver = driver;
    }

    protected override async Task<(Status, Session?)> CreateSession()
    {
        var response = await _driver.UnaryCall(QueryService.CreateSessionMethod, CreateSessionRequest,
            CreateSessionSettings);

        var status = Status.FromProto(response.Status, response.Issues);

        if (status.IsNotSuccess)
        {
            return (status, null);
        }

        TaskCompletionSource<(Status, Session?)> completeTask = new();

        var session = new Session(_driver, this, response.SessionId, response.NodeId);

        _ = Task.Run(async () =>
        {
            try
            {
                await using var stream = _driver.StreamCall(QueryService.AttachSessionMethod, new AttachSessionRequest
                    { SessionId = response.SessionId }, AttachSessionSettings);

                if (!await stream.MoveNextAsync())
                {
                    completeTask.SetResult(
                        (new Status(StatusCode.Cancelled, "Attach stream is not started!"), null)
                    ); // Session wasn't started!

                    return;
                }

                var statusSession = Status.FromProto(stream.Current.Status, stream.Current.Issues);

                if (statusSession.IsNotSuccess)
                {
                    completeTask.SetResult((statusSession, null));
                }

                completeTask.SetResult((status, session));

                try
                {
                    await foreach (var sessionState in stream) // watch attach stream session cycle life
                    {
                        session.OnStatus(Status.FromProto(sessionState.Status, sessionState.Issues));

                        if (!session.IsActive)
                        {
                            return;
                        }
                    }

                    // attach stream is closed
                    session.IsActive = false;
                }
                catch (Driver.TransportException e)
                {
                    session.OnStatus(e.Status);
                }
            }
            catch (Driver.TransportException e)
            {
                completeTask.SetResult((e.Status, null));
            }
        });

        return await completeTask.Task;
    }

    protected override async Task<Status> DeleteSession(string sessionId)
    {
        var deleteSessionResponse = await _driver.UnaryCall(QueryService.DeleteSessionMethod,
            new DeleteSessionRequest { SessionId = sessionId }, DeleteSessionSettings);

        return Status.FromProto(deleteSessionResponse.Status, deleteSessionResponse.Issues);
    }

    public ValueTask DisposeAsync()
    {
        return _driver.DisposeAsync();
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

    internal Driver.StreamIterator<Ydb.Query.ExecuteQueryResponsePart> ExecuteQuery(
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
            ExecMode = Ydb.Query.ExecMode.Execute,
            QueryContent = new QueryContent { Text = query, Syntax = (Ydb.Query.Syntax)settings.Syntax },
            StatsMode = Ydb.Query.StatsMode.None,
            TxControl = txControl
        };

        request.Parameters.Add(parameters.ToDictionary(p => p.Key, p => p.Value.GetProto()));

        return _driver.StreamCall(QueryService.ExecuteQueryMethod, request, settings);
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
