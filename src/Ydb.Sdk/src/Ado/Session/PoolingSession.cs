using Microsoft.Extensions.Logging;
using Ydb.Query;
using Ydb.Query.V1;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Value;
using CommitTransactionRequest = Ydb.Query.CommitTransactionRequest;
using TransactionControl = Ydb.Query.TransactionControl;

namespace Ydb.Sdk.Ado.Session;

internal class PoolingSession : IPoolingSession
{
    private const string SessionBalancer = "session-balancer";

    private static readonly TimeSpan DeleteSessionTimeout = TimeSpan.FromSeconds(5);
    private static readonly CreateSessionRequest CreateSessionRequest = new();

    private readonly IDriver _driver;
    private readonly PoolingSessionSource _poolingSessionSource;
    private readonly YdbConnectionStringBuilder _settings;
    private readonly ILogger<PoolingSession> _logger;

    private volatile bool _isBroken;

    private string SessionId { get; set; } = string.Empty;
    private long NodeId { get; set; }

    public bool IsBroken => _isBroken;

    internal PoolingSession(
        IDriver driver,
        PoolingSessionSource poolingSessionSource,
        YdbConnectionStringBuilder settings,
        ILogger<PoolingSession> logger
    )
    {
        _driver = driver;
        _poolingSessionSource = poolingSessionSource;
        _settings = settings;
        _logger = logger;
    }

    public ValueTask<IServerStream<ExecuteQueryResponsePart>> ExecuteQuery(
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

        return _driver.ServerStreamCall(QueryService.ExecuteQueryMethod, request, settings);
    }

    public async Task CommitTransaction(
        string txId,
        CancellationToken cancellationToken = default
    )
    {
        var response = await _driver.UnaryCall(
            QueryService.CommitTransactionMethod,
            new CommitTransactionRequest { SessionId = SessionId, TxId = txId },
            new GrpcRequestSettings { CancellationToken = cancellationToken, NodeId = NodeId }
        );

        if (response.Status.IsNotSuccess())
        {
            throw YdbException.FromServer(response.Status, response.Issues);
        }
    }

    public async Task RollbackTransaction(
        string txId,
        CancellationToken cancellationToken = default
    )
    {
        var response = await _driver.UnaryCall(
            QueryService.RollbackTransactionMethod,
            new RollbackTransactionRequest { SessionId = SessionId, TxId = txId },
            new GrpcRequestSettings { CancellationToken = cancellationToken, NodeId = NodeId }
        );

        if (response.Status.IsNotSuccess())
        {
            throw YdbException.FromServer(response.Status, response.Issues);
        }
    }

    public void OnNotSuccessStatusCode(StatusCode code)
    {
        if (code is
            StatusCode.Cancelled or
            StatusCode.BadSession or
            StatusCode.SessionBusy or
            StatusCode.InternalError or
            StatusCode.ClientTransportTimeout or
            StatusCode.Unavailable or
            StatusCode.ClientTransportUnavailable)
        {
            _logger.LogWarning("Session[{SessionId}] is deactivated. Reason StatusCode: {Code}", SessionId, code);

            _isBroken = true;
        }
    }

    public async Task Open(CancellationToken cancellationToken)
    {
        var requestSettings = new GrpcRequestSettings { CancellationToken = cancellationToken };

        if (!_settings.DisableServerBalancer)
        {
            requestSettings.ClientCapabilities.Add(SessionBalancer);
        }

        var response = await _driver.UnaryCall(QueryService.CreateSessionMethod, CreateSessionRequest, requestSettings);

        if (response.Status.IsNotSuccess())
        {
            throw YdbException.FromServer(response.Status, response.Issues);
        }

        TaskCompletionSource completeTask = new();

        SessionId = response.SessionId;
        NodeId = response.NodeId;

        _ = Task.Run(async () =>
        {
            try
            {
                using var stream = await _driver.ServerStreamCall(
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

                try
                {
                    // ReSharper disable once MethodSupportsCancellation
                    while (await stream.MoveNextAsync())
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
                    if (e.Code == StatusCode.Cancelled)
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

    public async Task DeleteSession()
    {
        try
        {
            if (_isBroken)
            {
                return;
            }

            _isBroken = true;

            var deleteSessionResponse = await _driver.UnaryCall(
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

    public void Close() => _poolingSessionSource.Return(this);
}
