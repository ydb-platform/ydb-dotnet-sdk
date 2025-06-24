using Microsoft.Extensions.Logging;
using Ydb.Query;
using Ydb.Query.V1;
using Ydb.Sdk.Pool;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Value;
using Ydb.Table;
using Ydb.Table.V1;
using CommitTransactionRequest = Ydb.Query.CommitTransactionRequest;
using CreateSessionRequest = Ydb.Query.CreateSessionRequest;
using DeleteSessionRequest = Ydb.Query.DeleteSessionRequest;
using DescribeTableResponse = Ydb.Table.DescribeTableResponse;
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
        var response = await _driver.UnaryCall(
            QueryService.CreateSessionMethod,
            CreateSessionRequest,
            new GrpcRequestSettings { CancellationToken = cancellationToken }
        );

        Status.FromProto(response.Status, response.Issues).EnsureSuccess();

        TaskCompletionSource<Status> completeTask = new();

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
                    completeTask.SetResult(new Status(StatusCode.Cancelled, "Attach stream is not started!"));

                    return;
                }

                completeTask.SetResult(Status.FromProto(stream.Current.Status, stream.Current.Issues));

                try
                {
                    // ReSharper disable once MethodSupportsCancellation
                    while (await stream.MoveNextAsync())
                    {
                        var sessionState = stream.Current;
                        var sessionStateStatus = Status.FromProto(sessionState.Status, sessionState.Issues);

                        Logger.LogDebug("Session[{SessionId}] was received the status from the attach stream: {Status}",
                            sessionId, sessionStateStatus);

                        session.OnStatus(sessionStateStatus);

                        // ReSharper disable once InvertIf
                        if (!session.IsActive)
                        {
                            return;
                        }
                    }

                    Logger.LogDebug("Session[{SessionId}]: Attached stream is closed", sessionId);

                    // attach stream is closed
                }
                catch (Driver.TransportException e)
                {
                    if (e.Status.StatusCode == StatusCode.Cancelled)
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

        (await completeTask.Task).EnsureSuccess();

        return session;
    }

    protected override ValueTask DisposeDriver() => _disposingDriver ? _driver.DisposeAsync() : default;
}

internal class Session : SessionBase<Session>
{
    internal IDriver Driver { get; }

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

    internal ValueTask<ServerStream<ExecuteQueryResponsePart>> ExecuteQuery(
        string query,
        Dictionary<string, YdbValue>? parameters,
        ExecuteQuerySettings? settings,
        TransactionControl? txControl)
    {
        parameters ??= new Dictionary<string, YdbValue>();
        settings = MakeGrpcRequestSettings(settings ?? new ExecuteQuerySettings());

        var request = new ExecuteQueryRequest
        {
            SessionId = SessionId,
            ExecMode = ExecMode.Execute,
            QueryContent = new QueryContent { Text = query, Syntax = (Ydb.Query.Syntax)settings.Syntax },
            StatsMode = StatsMode.None,
            TxControl = txControl
        };

        request.Parameters.Add(parameters.ToDictionary(p => p.Key, p => p.Value.GetProto()));

        return Driver.ServerStreamCall(QueryService.ExecuteQueryMethod, request, settings);
    }

    internal async Task<Status> CommitTransaction(string txId, GrpcRequestSettings? settings = null)
    {
        settings = MakeGrpcRequestSettings(settings ?? new GrpcRequestSettings());

        var response = await Driver.UnaryCall(QueryService.CommitTransactionMethod,
            new CommitTransactionRequest { SessionId = SessionId, TxId = txId }, settings);

        return Status.FromProto(response.Status, response.Issues);
    }

    internal async Task<Status> RollbackTransaction(string txId, GrpcRequestSettings? settings = null)
    {
        settings = MakeGrpcRequestSettings(settings ?? new GrpcRequestSettings());

        var response = await Driver.UnaryCall(QueryService.RollbackTransactionMethod,
            new RollbackTransactionRequest { SessionId = SessionId, TxId = txId }, settings);

        return Status.FromProto(response.Status, response.Issues);
    }

    internal async Task<DescribeTableResponse> DescribeTable(string path, DescribeTableSettings? settings = null)
    {
        settings = MakeGrpcRequestSettings(settings ?? new DescribeTableSettings());

        return await Driver.UnaryCall(
            TableService.DescribeTableMethod,
            new DescribeTableRequest
            {
                Path = path,
                IncludeTableStats = settings.IncludeTableStats,
                IncludePartitionStats = settings.IncludePartitionStats,
                IncludeShardKeyBounds = settings.IncludeShardKeyBounds
            },
            settings
        );
    }

    internal override async Task<Status> DeleteSession()
    {
        try
        {
            IsActive = false;

            var settings = MakeGrpcRequestSettings(new GrpcRequestSettings
                { TransportTimeout = TimeSpan.FromSeconds(5) });

            var deleteSessionResponse = await Driver.UnaryCall(
                QueryService.DeleteSessionMethod,
                new DeleteSessionRequest { SessionId = SessionId },
                settings
            );

            return Status.FromProto(deleteSessionResponse.Status, deleteSessionResponse.Issues);
        }
        catch (Driver.TransportException e)
        {
            return e.Status;
        }
    }
}
