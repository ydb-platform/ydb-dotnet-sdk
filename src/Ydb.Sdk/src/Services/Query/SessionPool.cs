using Microsoft.Extensions.Logging;
using Ydb.Query;
using Ydb.Query.V1;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Ado.Session;
using Ydb.Sdk.Pool;
using Ydb.Sdk.Value;
using Ydb.Table;
using Ydb.Table.V1;
using CommitTransactionRequest = Ydb.Query.CommitTransactionRequest;
using CreateSessionRequest = Ydb.Query.CreateSessionRequest;
using DeleteSessionRequest = Ydb.Query.DeleteSessionRequest;
using RollbackTransactionRequest = Ydb.Query.RollbackTransactionRequest;
using TransactionControl = Ydb.Query.TransactionControl;

namespace Ydb.Sdk.Services.Query;

internal sealed class SessionPool : SessionPool<Session>, IAsyncDisposable
{
    private static readonly CreateSessionRequest CreateSessionRequest = new();

    private readonly IDriver _driver;
    private readonly bool _disposingDriver;
    private readonly ILogger<Session> _loggerSession;

    internal SessionPool(IDriver driver, SessionPoolConfig sessionPoolConfig)
        : base(driver.LoggerFactory.CreateLogger<SessionPool>(), sessionPoolConfig)
    {
        _driver = driver;
        _disposingDriver = sessionPoolConfig.DisposeDriver;
        _loggerSession = driver.LoggerFactory.CreateLogger<Session>();
    }

    protected override async Task<Session> CreateSession(
        CancellationToken cancellationToken = default
    )
    {
        var requestSettings = new GrpcRequestSettings
        {
            CancellationToken = cancellationToken
        };

        if (!Config.DisableServerBalancer)
        {
            requestSettings.ClientCapabilities.Add("session-balancer");
        }

        var response = await _driver.UnaryCall(
            QueryService.CreateSessionMethod,
            CreateSessionRequest,
            requestSettings
        );

        Status.FromProto(response.Status, response.Issues).EnsureSuccess();

        TaskCompletionSource completeTask = new();

        var sessionId = response.SessionId;
        var nodeId = response.NodeId;

        var session = new Session(_driver, this, sessionId, nodeId, _loggerSession);

        _ = Task.Run(async () =>
        {
            try
            {
                using var stream = await _driver.ServerStreamCall(
                    QueryService.AttachSessionMethod,
                    new AttachSessionRequest { SessionId = sessionId },
                    new GrpcRequestSettings { NodeId = nodeId }
                );

                if (!await stream.MoveNextAsync(cancellationToken))
                {
                    // Session wasn't started!
                    completeTask.SetException(new YdbException(StatusCode.Cancelled, "Attach stream is not started!"));

                    return;
                }

                if (stream.Current.Status.IsNotSuccess())
                {
                    completeTask.SetException(YdbException.FromServer(stream.Current.Status, stream.Current.Issues));
                }

                completeTask.SetResult();

                try
                {
                    // ReSharper disable once MethodSupportsCancellation
                    while (await stream.MoveNextAsync())
                    {
                        var sessionState = stream.Current;

                        var statusCode = sessionState.Status.Code();

                        Logger.LogDebug("Session[{SessionId}] was received the status from the attach stream: {Code}",
                            sessionId, statusCode);

                        session.OnNotSuccessStatusCode(statusCode);

                        // ReSharper disable once InvertIf
                        if (!session.IsActive)
                        {
                            return;
                        }
                    }

                    Logger.LogDebug("Session[{SessionId}]: Attached stream is closed", sessionId);

                    // attach stream is closed
                }
                catch (YdbException e)
                {
                    if (e.Code == StatusCode.Cancelled)
                    {
                        Logger.LogDebug("AttachStream is cancelled (possible grpcChannel is closing)");

                        return;
                    }

                    Logger.LogWarning(e, "Session[{SessionId}] is deactivated by transport error", sessionId);
                }
            }
            catch (Exception e)
            {
                completeTask.SetException(e);
            }
            finally
            {
                session.IsActive = false;
            }
        }, cancellationToken);

        await completeTask.Task;

        return session;
    }

    protected override ValueTask DisposeDriver() => _disposingDriver ? _driver.DisposeAsync() : default;
}

internal class Session : SessionBase<Session>, ISession
{
    internal Session(
        IDriver driver,
        SessionPool<Session> sessionPool,
        string sessionId,
        long nodeId,
        ILogger<Session> logger
    ) : base(sessionPool, sessionId, nodeId, logger)
    {
        Driver = driver;
    }

    public IDriver Driver { get; }

    public ValueTask<IServerStream<ExecuteQueryResponsePart>> ExecuteQuery(
        string query,
        Dictionary<string, YdbValue> parameters,
        GrpcRequestSettings settings,
        TransactionControl? txControl)
    {
        settings = MakeGrpcRequestSettings(settings);

        var request = new ExecuteQueryRequest
        {
            SessionId = SessionId,
            ExecMode = ExecMode.Execute,
            QueryContent = new QueryContent { Text = query, Syntax = Ydb.Query.Syntax.YqlV1 },
            StatsMode = StatsMode.None,
            TxControl = txControl
        };

        request.Parameters.Add(parameters.ToDictionary(p => p.Key, p => p.Value.GetProto()));

        return Driver.ServerStreamCall(QueryService.ExecuteQueryMethod, request, settings);
    }

    public bool IsBroken => !IsActive;

    public new void OnNotSuccessStatusCode(StatusCode code) => base.OnNotSuccessStatusCode(code);

    public void Close() => Release();

    public async Task CommitTransaction(string txId, CancellationToken cancellationToken = default)
    {
        var settings = MakeGrpcRequestSettings(new GrpcRequestSettings { CancellationToken = cancellationToken });

        var response = await Driver.UnaryCall(QueryService.CommitTransactionMethod,
            new CommitTransactionRequest { SessionId = SessionId, TxId = txId }, settings);

        if (response.Status.IsNotSuccess())
        {
            throw YdbException.FromServer(response.Status, response.Issues);
        }
    }

    public async Task RollbackTransaction(string txId, CancellationToken cancellationToken = default)
    {
        var settings = MakeGrpcRequestSettings(new GrpcRequestSettings { CancellationToken = cancellationToken });

        var response = await Driver.UnaryCall(QueryService.RollbackTransactionMethod,
            new RollbackTransactionRequest { SessionId = SessionId, TxId = txId }, settings);

        if (response.Status.IsNotSuccess())
        {
            throw YdbException.FromServer(response.Status, response.Issues);
        }
    }

    internal override async Task DeleteSession()
    {
        IsActive = false;

        var settings = MakeGrpcRequestSettings(new GrpcRequestSettings
            { TransportTimeout = TimeSpan.FromSeconds(5) });

        var deleteSessionResponse = await Driver.UnaryCall(
            QueryService.DeleteSessionMethod,
            new DeleteSessionRequest { SessionId = SessionId },
            settings
        );

        if (deleteSessionResponse.Status.IsNotSuccess())
        {
            throw YdbException.FromServer(deleteSessionResponse.Status, deleteSessionResponse.Issues);
        }
    }
    
    internal async Task<BulkUpsertResponse> BulkUpsertAsync(BulkUpsertRequest req, CancellationToken ct = default)
    {
        var settings = MakeGrpcRequestSettings(new GrpcRequestSettings { CancellationToken = ct });
        return await Driver.UnaryCall(TableService.BulkUpsertMethod, req, settings);
    }
}
