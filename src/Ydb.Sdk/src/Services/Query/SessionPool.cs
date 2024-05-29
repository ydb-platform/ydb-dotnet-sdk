using Microsoft.Extensions.Logging;
using Ydb.Sdk.Pool;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query;

internal class SessionPool : SessionPool<Session>, IAsyncDisposable
{
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

    private readonly QueryServiceRpc _rpc;

    internal SessionPool(QueryServiceRpc rpc, ILogger<SessionPool<Session>> logger, int? size = null) 
        : base(logger, size)
    {
        _rpc = rpc;
    }

    protected override async Task<(Status, Session?)> CreateSession()
    {
        var response = await _rpc.CreateSession(CreateSessionSettings);

        var status = Status.FromProto(response.Status, response.Issues);

        if (status.IsNotSuccess)
        {
            return (status, null);
        }

        TaskCompletionSource<(Status, Session?)> completeTask = new();

        var session = new Session(_rpc, this, response.SessionId, response.NodeId);

        _ = Task.Run(async () =>
        {
            await using var stream = _rpc.AttachSession(response.SessionId, AttachSessionSettings);

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

            await foreach (var sessionState in stream)
            {
                session.OnStatus(Status.FromProto(sessionState.Status, sessionState.Issues));

                if (!session.IsActive)
                {
                    return;
                }
            }
        });

        return await completeTask.Task;
    }

    protected override async Task<Status> DeleteSession()
    {
        var deleteSessionResponse = await _rpc.DeleteSession(DeleteSessionSettings);

        return Status.FromProto(deleteSessionResponse.Status, deleteSessionResponse.Issues);
    }

    public ValueTask DisposeAsync()
    {
        return _rpc.DisposeAsync();
    }
}

public class Session : SessionBase<Session>
{
    private readonly QueryServiceRpc _rpc;

    internal Session(QueryServiceRpc rpc, SessionPool<Session> sessionPool, string sessionId, long nodeId)
        : base(sessionPool, sessionId, nodeId)
    {
        _rpc = rpc;
    }

    public async IAsyncEnumerable<(Status, Ydb.ResultSet?)> ExecuteQuery(string query,
        Dictionary<string, YdbValue>? parameters = null,
        TxMode txMode = TxMode.None, ExecuteQuerySettings? settings = null)
    {
        parameters ??= new Dictionary<string, YdbValue>();
        settings ??= new ExecuteQuerySettings();

        settings.NodeId = NodeId;

        await foreach (var resultPart in _rpc.ExecuteQuery(query, SessionId, txMode, parameters, settings))
        {
            var status = Status.FromProto(resultPart.Status, resultPart.Issues);

            OnStatus(status);

            yield return (status, resultPart.ResultSet);
        }
    }

    public async Task<(Status, string?)> BeginTransaction(TxMode txMode = TxMode.SerializableRw,
        GrpcRequestSettings? settings = null)
    {
        settings ??= GrpcRequestSettings.DefaultInstance;
        settings.NodeId = NodeId;

        var response = await _rpc.BeginTransaction(SessionId, txMode, settings);

        var status = Status.FromProto(response.Status, response.Issues);

        return status.IsSuccess ? (status, response.TxMeta.Id) : (status, null);
    }

    public async Task<Status> CommitTransaction(string txId, GrpcRequestSettings? settings = null)
    {
        settings ??= GrpcRequestSettings.DefaultInstance;
        settings.NodeId = NodeId;

        var response = await _rpc.CommitTransaction(SessionId, txId, settings);

        var status = Status.FromProto(response.Status, response.Issues);

        OnStatus(status);

        return status;
    }

    public async Task<Status> RollbackTransaction(string txId, GrpcRequestSettings? settings = null)
    {
        settings ??= GrpcRequestSettings.DefaultInstance;
        settings.NodeId = NodeId;

        var response = await _rpc.RollbackTransaction(SessionId, txId, settings);

        var status = Status.FromProto(response.Status, response.Issues);

        OnStatus(status);

        return status;
    }
}
