using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Ydb.Query;
using Ydb.Query.V1;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Tracing;
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

    private int _isBroken = 1;
    private volatile bool _isBadSession;

    private string SessionId { get; set; } = string.Empty;
    private long NodeId { get; set; }

    public override IDriver Driver { get; }
    public override bool IsBroken => Volatile.Read(ref _isBroken) != 0;

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
        Dictionary<string, TypedValue> parameters,
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
        request.Parameters.Add(parameters);

        return Driver.ServerStreamCall(QueryService.ExecuteQueryMethod, request, settings);
    }

    public override async Task CommitTransaction(
        string txId,
        Activity? dbActivity,
        CancellationToken cancellationToken
    )
    {
        var response = await Driver.UnaryCall(
            QueryService.CommitTransactionMethod,
            new CommitTransactionRequest { SessionId = SessionId, TxId = txId },
            new GrpcRequestSettings { CancellationToken = cancellationToken, NodeId = NodeId, DbActivity = dbActivity }
        ).ConfigureAwait(false);

        if (response.Status.IsNotSuccess())
        {
            throw YdbException.FromServer(response.Status, response.Issues);
        }
    }

    public override async Task RollbackTransaction(
        string txId,
        Activity? dbActivity,
        CancellationToken cancellationToken
    )
    {
        var response = await Driver.UnaryCall(
            QueryService.RollbackTransactionMethod,
            new RollbackTransactionRequest { SessionId = SessionId, TxId = txId },
            new GrpcRequestSettings { CancellationToken = cancellationToken, NodeId = NodeId, DbActivity = dbActivity }
        ).ConfigureAwait(false);

        if (response.Status.IsNotSuccess())
        {
            throw YdbException.FromServer(response.Status, response.Issues);
        }
    }

    public override void OnNotSuccessStatusCode(StatusCode statusCode)
    {
        SessionClosedReason reason;
        switch (statusCode)
        {
            case StatusCode.BadSession:
            case StatusCode.SessionExpired:
                _isBadSession = true;
                reason = SessionClosedReason.BadSession;
                break;
            case StatusCode.SessionBusy:
                reason = SessionClosedReason.SessionBusy;
                break;
            case StatusCode.ClientTransportTimeout:
                reason = SessionClosedReason.ClientTimeout;
                break;
            case StatusCode.ClientCancelled:
                reason = SessionClosedReason.ClientCancelled;
                break;
            case StatusCode.ClientTransportUnavailable:
            case StatusCode.ClientTransportResourceExhausted:
            case StatusCode.ClientTransportUnknown:
                reason = SessionClosedReason.TransportError;
                break;
            default:
                return;
        }

        _logger.LogWarning("Session[{SessionId}] is deactivated. Reason Status: {Status}", SessionId, statusCode);
        BrokenSession(reason);
    }

    private void BrokenSession(SessionClosedReason reason)
    {
        if (Interlocked.CompareExchange(ref _isBroken, 1, 0) == 0)
            MetricsReporter.ReportSessionClosed(reason);
    }

    internal override async Task Open(CancellationToken cancellationToken)
    {
        var startTimestamp = YdbMetricsReporter.ReportConnectionCreateTimeStart();
        using var dbActivity = YdbActivitySource.StartActivity("ydb.CreateSession");

        try
        {
            var requestSettings = new GrpcRequestSettings
                { CancellationToken = cancellationToken, DbActivity = dbActivity };

            if (!_disableServerBalancer)
            {
                requestSettings.ClientCapabilities.Add(SessionBalancer);
            }

            var response = await Driver
                .UnaryCall(QueryService.CreateSessionMethod, CreateSessionRequest, requestSettings)
                .ConfigureAwait(false);

            if (response.Status.IsNotSuccess())
            {
                throw YdbException.FromServer(response.Status, response.Issues);
            }

            SessionId = response.SessionId;
            NodeId = response.NodeId;

            var stream = await Driver.ServerStreamCall(
                QueryService.AttachSessionMethod,
                new AttachSessionRequest { SessionId = SessionId },
                new GrpcRequestSettings { NodeId = NodeId }
            ).ConfigureAwait(false);

            try
            {
                if (!await stream.MoveNextAsync(cancellationToken).ConfigureAwait(false))
                {
                    throw new YdbException(StatusCode.Cancelled, "Attach stream is not started!");
                }

                var initSessionState = stream.Current;

                if (initSessionState.Status.IsNotSuccess())
                {
                    throw YdbException.FromServer(initSessionState.Status, initSessionState.Issues);
                }
            }
            catch
            {
                stream.Dispose();
                throw;
            }

            Volatile.Write(ref _isBroken, 0);
            _ = ProcessAttachStream(stream);
        }
        catch (YdbException e)
        {
            dbActivity?.SetException(e);
            MetricsReporter.ReportOperationFailed(e.Code, "CreateSession");
            throw;
        }
        finally
        {
            MetricsReporter.ReportConnectionCreateTime(startTimestamp);
        }
    }

    private async Task ProcessAttachStream(IServerStream<SessionState> stream)
    {
        using (stream)
        {
            try
            {
                // ReSharper disable once MethodSupportsCancellation
                while (await stream.MoveNextAsync(_attachStreamLifecycleCts.Token).ConfigureAwait(false))
                {
                    var sessionState = stream.Current;
                    var statusCode = sessionState.Status.Code();

                    switch (sessionState.SessionHintCase)
                    {
                        case SessionState.SessionHintOneofCase.NodeShutdown:
                            Driver.PessimizeNode(NodeId);
                            _isBadSession = true;
                            BrokenSession(SessionClosedReason.NodeShutdown);
                            break;
                        case SessionState.SessionHintOneofCase.SessionShutdown:
                            _isBadSession = true;
                            BrokenSession(SessionClosedReason.SessionShutdown);
                            break;
                        case SessionState.SessionHintOneofCase.None:
                        default:
                            OnNotSuccessStatusCode(statusCode);
                            break;
                    }

                    _logger.LogDebug(
                        "Session[{SessionId}] was received the status from the attach stream: {StatusMessage}, " +
                        "hint: {Hint}",
                        SessionId, statusCode.ToMessage(sessionState.Issues), sessionState.SessionHintCase);

                    if (IsBroken)
                    {
                        return;
                    }
                }

                _logger.LogDebug("Session[{SessionId}]: Attached stream is closed", SessionId);

                BrokenSession(SessionClosedReason.AttachClosed);
            }
            catch (YdbException e)
            {
                if (e.Code == StatusCode.ClientTransportTimeout)
                {
                    _logger.LogDebug("AttachStream is cancelled (possible grpcChannel is closing)");

                    return;
                }

                _logger.LogWarning(e, "Session[{SessionId}] is deactivated by transport error", SessionId);
                BrokenSession(SessionClosedReason.TransportError);
            }
        }
    }

    internal override async Task DeleteSession(SessionClosedReason reason)
    {
        try
        {
            _attachStreamLifecycleCts.CancelAfter(DeleteSessionTimeout);

            if (_isBadSession)
            {
                return;
            }

            BrokenSession(reason);
            _isBadSession = true;
            var deleteSessionResponse = await Driver.UnaryCall(
                QueryService.DeleteSessionMethod,
                new DeleteSessionRequest { SessionId = SessionId },
                new GrpcRequestSettings { TransportTimeout = DeleteSessionTimeout, NodeId = NodeId }
            ).ConfigureAwait(false);

            if (deleteSessionResponse.Status.IsNotSuccess())
            {
                _logger.LogWarning("Failed to delete session[{SessionId}], {StatusMessage}", SessionId,
                    deleteSessionResponse.Status.Code().ToMessage(deleteSessionResponse.Issues));
            }
        }
        catch (Exception e)
        {
            _logger.LogDebug(e, "Error occurred while deleting session[{SessionId}] (NodeId = {NodeId})",
                SessionId, NodeId);
        }
    }
}
