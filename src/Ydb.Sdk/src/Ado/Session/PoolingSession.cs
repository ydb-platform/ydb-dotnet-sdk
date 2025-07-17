// This file contains session pooling algorithms adapted from Npgsql
// Original source: https://github.com/npgsql/npgsql
// Copyright (c) 2002-2025, Npgsql
// Licence https://github.com/npgsql/npgsql?tab=PostgreSQL-1-ov-file

using Microsoft.Extensions.Logging;
using Ydb.Query;
using Ydb.Query.V1;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Value;
using CommitTransactionRequest = Ydb.Query.CommitTransactionRequest;
using TransactionControl = Ydb.Query.TransactionControl;

namespace Ydb.Sdk.Ado.Session;

internal class PoolingSession : PoolingSessionBase<PoolingSession>
{
    private const string SessionBalancer = "session-balancer";

    private static readonly TimeSpan DeleteSessionTimeout = TimeSpan.FromSeconds(5);
    private static readonly CreateSessionRequest CreateSessionRequest = new();

    private readonly ILogger<PoolingSession> _logger;
    private readonly bool _disableServerBalancer;

    private readonly CancellationTokenSource _attachStreamLifecycleCts = new();

    private volatile bool _isBroken = true;
    private volatile bool _isBadSession;

    private string SessionId { get; set; } = string.Empty;
    private long NodeId { get; set; }

    public override IDriver Driver { get; }
    public override bool IsBroken => _isBroken;

    internal PoolingSession(
        IDriver driver,
        PoolingSessionSource<PoolingSession> poolingSessionSource,
        bool disableServerBalancer,
        ILogger<PoolingSession> logger
    ) : base(poolingSessionSource)
    {
        _disableServerBalancer = disableServerBalancer;
        _logger = logger;
        Driver = driver;
    }

    public override ValueTask<IServerStream<ExecuteQueryResponsePart>> ExecuteQuery(
        string query,
        Dictionary<string, YdbValue> parameters,
        GrpcRequestSettings settings,
        TransactionControl? txControl
    )
    {
        settings.NodeId = NodeId;

        var request = new ExecuteQueryRequest
        {
            SessionId = SessionId,
            ExecMode = ExecMode.Execute,
            QueryContent = new QueryContent { Text = query, Syntax = Syntax.YqlV1 },
            StatsMode = StatsMode.None,
            TxControl = txControl
        };
        request.Parameters.Add(parameters.ToDictionary(p => p.Key, p => p.Value.GetProto()));

        return Driver.ServerStreamCall(QueryService.ExecuteQueryMethod, request, settings);
    }

    public override async Task CommitTransaction(string txId, CancellationToken cancellationToken = default)
    {
        var response = await Driver.UnaryCall(
            QueryService.CommitTransactionMethod,
            new CommitTransactionRequest { SessionId = SessionId, TxId = txId },
            new GrpcRequestSettings { CancellationToken = cancellationToken, NodeId = NodeId }
        );

        if (response.Status.IsNotSuccess())
        {
            throw YdbException.FromServer(response.Status, response.Issues);
        }
    }

    public override async Task RollbackTransaction(string txId, CancellationToken cancellationToken = default)
    {
        var response = await Driver.UnaryCall(
            QueryService.RollbackTransactionMethod,
            new RollbackTransactionRequest { SessionId = SessionId, TxId = txId },
            new GrpcRequestSettings { CancellationToken = cancellationToken, NodeId = NodeId }
        );

        if (response.Status.IsNotSuccess())
        {
            throw YdbException.FromServer(response.Status, response.Issues);
        }
    }

    public override void OnNotSuccessStatusCode(StatusCode statusCode)
    {
        _isBadSession = _isBadSession || statusCode is StatusCode.BadSession;

        if (statusCode is
            StatusCode.BadSession or
            StatusCode.SessionBusy or
            StatusCode.SessionExpired or
            StatusCode.ClientTransportTimeout or
            StatusCode.ClientTransportUnavailable)
        {
            _logger.LogWarning("Session[{SessionId}] is deactivated. Reason Status: {Status}", SessionId, statusCode);

            _isBroken = true;
        }
    }

    internal override async Task Open(CancellationToken cancellationToken)
    {
        var requestSettings = new GrpcRequestSettings { CancellationToken = cancellationToken };

        if (!_disableServerBalancer)
        {
            requestSettings.ClientCapabilities.Add(SessionBalancer);
        }

        var response = await Driver.UnaryCall(QueryService.CreateSessionMethod, CreateSessionRequest, requestSettings);

        if (response.Status.IsNotSuccess())
        {
            throw YdbException.FromServer(response.Status, response.Issues);
        }

        TaskCompletionSource completeTask = new();

        SessionId = response.SessionId;
        NodeId = response.NodeId;
        _isBroken = false;

        _ = Task.Run(async () =>
        {
            try
            {
                using var stream = await Driver.ServerStreamCall(
                    QueryService.AttachSessionMethod,
                    new AttachSessionRequest { SessionId = SessionId },
                    new GrpcRequestSettings { NodeId = NodeId }
                );

                if (!await stream.MoveNextAsync(cancellationToken))
                {
                    // Session wasn't started!
                    completeTask.SetException(new YdbException(StatusCode.Cancelled, "Attach stream is not started!"));

                    return;
                }

                var initSessionState = stream.Current;

                if (initSessionState.Status.IsNotSuccess())
                {
                    throw YdbException.FromServer(initSessionState.Status, initSessionState.Issues);
                }

                completeTask.SetResult();

                var lifecycleAttachToken = _attachStreamLifecycleCts.Token;

                try
                {
                    // ReSharper disable once MethodSupportsCancellation
                    while (await stream.MoveNextAsync(lifecycleAttachToken))
                    {
                        var sessionState = stream.Current;

                        var statusCode = sessionState.Status.Code();

                        _logger.LogDebug(
                            "Session[{SessionId}] was received the status from the attach stream: {StatusMessage}",
                            SessionId, statusCode.ToMessage(sessionState.Issues));

                        OnNotSuccessStatusCode(statusCode);

                        if (IsBroken)
                        {
                            return;
                        }
                    }

                    _logger.LogDebug("Session[{SessionId}]: Attached stream is closed", SessionId);

                    // attach stream is closed
                }
                catch (YdbException e)
                {
                    if (e.Code == StatusCode.ClientTransportTimeout)
                    {
                        _logger.LogDebug("AttachStream is cancelled (possible grpcChannel is closing)");

                        return;
                    }

                    _logger.LogWarning(e, "Session[{SessionId}] is deactivated by transport error", SessionId);
                }
            }
            catch (Exception e)
            {
                completeTask.SetException(e);
            }
            finally
            {
                _isBroken = true;
            }
        }, cancellationToken);

        await completeTask.Task;
    }

    internal override async Task DeleteSession()
    {
        try
        {
            _isBroken = true;
            _attachStreamLifecycleCts.CancelAfter(DeleteSessionTimeout);

            if (_isBadSession)
            {
                return;
            }

            _isBadSession = true;
            var deleteSessionResponse = await Driver.UnaryCall(
                QueryService.DeleteSessionMethod,
                new DeleteSessionRequest { SessionId = SessionId },
                new GrpcRequestSettings { TransportTimeout = DeleteSessionTimeout, NodeId = NodeId }
            );

            if (deleteSessionResponse.Status.IsNotSuccess())
            {
                _logger.LogWarning("Failed to delete session[{SessionId}], {StatusMessage}", SessionId,
                    deleteSessionResponse.Status.Code().ToMessage(deleteSessionResponse.Issues));
            }
        }
        catch (Exception e)
        {
            _logger.LogWarning(e, "Error occurred while deleting session[{SessionId}] (NodeId = {NodeId})",
                SessionId, NodeId);
        }
    }
}
