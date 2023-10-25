using Microsoft.Extensions.Logging;
using Ydb.Query;
using Ydb.Query.V1;
using Ydb.Sdk.Client;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query;

public enum TxMode
{
    SerializableRW,
    OnlineRO,
    StaleRO
}

public enum ExecMode
{
    Unspecified = 0,
    Parse = 10,
    Validate = 20,
    Explain = 30,

    // reserved 40; // EXEC_MODE_PREPARE
    Execute = 50
}

public enum Syntax
{
    Unspecified = 0,

    /// <summary>
    /// YQL
    /// </summary>
    YqlV1 = 1,

    /// <summary>
    /// PostgresQL
    /// </summary>
    Pg = 2
}

public enum StatsMode
{
    Unspecified = 0,

    /// <summary>
    /// Stats collection is disabled
    /// </summary>
    None = 10,

    /// <summary>
    /// Aggregated stats of reads, updates and deletes per table
    /// </summary>
    Basic = 20,

    /// <summary>
    /// Add execution stats and plan on top of STATS_MODE_BASIC
    /// </summary>
    Full = 30,

    /// <summary>
    /// Detailed execution stats including stats for individual tasks and channels
    /// </summary>
    Profile = 40
}

public class ExecuteQuerySettings : RequestSettings
{
    public ExecMode ExecMode { get; set; } = ExecMode.Execute;
    public Syntax Syntax { get; set; }

    public StatsMode StatsMode { get; set; }
}

public class CreateSessionSettings : RequestSettings
{
}

public class DeleteSessionSettings : RequestSettings
{
}

public class AttachSessionSettings : RequestSettings
{
}

public class BeginTransactionSettings : RequestSettings
{
}

public class CommitTransactionSettings : RequestSettings
{
}

public class RollbackTransactionSettings : RequestSettings
{
}

public class CreateSessionResponse : ResponseBase
{
    public Session? Session { get; }

    internal CreateSessionResponse(Status status) : base(status)
    {
    }

    private CreateSessionResponse(Ydb.Query.CreateSessionResponse proto, Session? session = null)
        : base(Status.FromProto(proto.Status, proto.Issues))
    {
        Session = session;
    }

    internal static CreateSessionResponse FromProto(Ydb.Query.CreateSessionResponse proto, Driver driver,
        string endpoint)
    {
        var session = new Session(
            driver: driver,
            sessionPool: null,
            id: proto.SessionId,
            nodeId: proto.NodeId,
            endpoint: endpoint
        );
        return new CreateSessionResponse(proto, session);
    }
}

public class DeleteSessionResponse : ResponseBase
{
    internal DeleteSessionResponse(Status status) : base(status)
    {
    }

    private DeleteSessionResponse(Ydb.Query.DeleteSessionResponse proto)
        : base(Status.FromProto(proto.Status, proto.Issues))
    {
    }

    internal static DeleteSessionResponse FromProto(Ydb.Query.DeleteSessionResponse proto)
    {
        return new DeleteSessionResponse(proto);
    }
}

public class SessionState : ResponseBase
{
    internal SessionState(Status status) : base(status)
    {
    }

    private SessionState(Ydb.Query.SessionState proto)
        : base(Status.FromProto(proto.Status, proto.Issues))
    {
    }

    internal static SessionState FromProto(Ydb.Query.SessionState proto)
    {
        return new SessionState(proto);
    }
}

public class SessionStateStream : StreamResponse<Ydb.Query.SessionState, SessionState>
{
    internal SessionStateStream(Driver.StreamIterator<Ydb.Query.SessionState> iterator) : base(iterator)
    {
    }

    protected override SessionState MakeResponse(Ydb.Query.SessionState protoResponse)
    {
        return SessionState.FromProto(protoResponse);
    }

    protected override SessionState MakeResponse(Status status)
    {
        return new SessionState(status);
    }
}

public class QueryStats
{
}

public class ExecuteQueryResponsePart : ResponseBase
{
    public long? ResultSetIndex;
    public Value.ResultSet? ResultSet;

    internal ExecuteQueryResponsePart(Status status) : base(status)
    {
    }

    private ExecuteQueryResponsePart(Ydb.Query.ExecuteQueryResponsePart proto)
        : base(Status.FromProto(proto.Status, proto.Issues))
    {
        ResultSetIndex = proto.ResultSetIndex;
        if (proto.ResultSet is not null)
        {
            ResultSet = Value.ResultSet.FromProto(proto.ResultSet);
        }
    }

    internal static ExecuteQueryResponsePart FromProto(Ydb.Query.ExecuteQueryResponsePart proto)
    {
        return new ExecuteQueryResponsePart(proto);
    }
}

public class ExecuteQueryStream : StreamResponse<Ydb.Query.ExecuteQueryResponsePart, ExecuteQueryResponsePart>
    , IAsyncEnumerable<ExecuteQueryResponsePart>, IAsyncEnumerator<ExecuteQueryResponsePart>
{
    public object ExecStats { get; }

    internal ExecuteQueryStream(Driver.StreamIterator<Ydb.Query.ExecuteQueryResponsePart> iterator) : base(iterator)
    {
    }

    protected override ExecuteQueryResponsePart MakeResponse(Ydb.Query.ExecuteQueryResponsePart protoResponse)
    {
        return ExecuteQueryResponsePart.FromProto(protoResponse);
    }

    protected override ExecuteQueryResponsePart MakeResponse(Status status)
    {
        return new ExecuteQueryResponsePart(status);
    }

    public IAsyncEnumerator<ExecuteQueryResponsePart> GetAsyncEnumerator(
        CancellationToken cancellationToken = new CancellationToken())
    {
        throw new NotImplementedException();
    }

    public ValueTask DisposeAsync()
    {
        throw new NotImplementedException();
    }

    public ValueTask<bool> MoveNextAsync()
    {
        throw new NotImplementedException();
    }

    public ExecuteQueryResponsePart Current { get; }
}

public class BeginTransactionResponse : ResponseBase
{
    internal BeginTransactionResponse(Status status) : base(status)
    {
    }

    public Tx Tx { get; } = new();

    private BeginTransactionResponse(Ydb.Query.BeginTransactionResponse proto) : base(
        Status.FromProto(proto.Status, proto.Issues))
    {
        Tx.TxId = proto.TxMeta.Id;
    }

    internal static BeginTransactionResponse FromProto(Ydb.Query.BeginTransactionResponse proto)
    {
        return new BeginTransactionResponse(proto);
    }
}

public class CommitTransactionResponse : ResponseBase
{
    internal CommitTransactionResponse(Status status) : base(status)
    {
    }

    private CommitTransactionResponse(Ydb.Query.CommitTransactionResponse proto) : base(
        Status.FromProto(proto.Status, proto.Issues))
    {
    }

    internal static CommitTransactionResponse FromProto(Ydb.Query.CommitTransactionResponse proto)
    {
        return new CommitTransactionResponse(proto);
    }
}

public class RollbackTransactionResponse : ResponseBase
{
    internal RollbackTransactionResponse(Status status) : base(status)
    {
    }

    private RollbackTransactionResponse(Ydb.Query.RollbackTransactionResponse proto) : base(
        Status.FromProto(proto.Status, proto.Issues))
    {
    }

    internal static RollbackTransactionResponse FromProto(Ydb.Query.RollbackTransactionResponse proto)
    {
        return new RollbackTransactionResponse(proto);
    }
}

public class QueryClientConfig
{
    public SessionPoolConfig SessionPoolConfig { get; }

    public QueryClientConfig(
        SessionPoolConfig? sessionPoolConfig = null)
    {
        SessionPoolConfig = sessionPoolConfig ?? new SessionPoolConfig();
    }
}

public class QueryClient :
    ClientBase,
    IDisposable
{
    private readonly ISessionPool _sessionPool;
    private readonly ILogger _logger;
    private bool _disposed;

    public QueryClient(Driver driver, QueryClientConfig? config = null) : base(driver)
    {
        config ??= new QueryClientConfig();

        _logger = Driver.LoggerFactory.CreateLogger<QueryClient>();

        _sessionPool = new SessionPool(driver, config.SessionPoolConfig);
    }

    internal QueryClient(Driver driver, ISessionPool sessionPool) : base(driver)
    {
        _logger = driver.LoggerFactory.CreateLogger<QueryClient>();

        _sessionPool = sessionPool;
    }

    public async Task<CreateSessionResponse> CreateSession(CreateSessionSettings? settings = null)
    {
        settings ??= new CreateSessionSettings();
        var request = new CreateSessionRequest();

        try
        {
            var response = await Driver.UnaryCall(
                method: QueryService.CreateSessionMethod,
                request: request,
                settings: settings);


            return CreateSessionResponse.FromProto(response.Data, Driver, response.UsedEndpoint);
        }
        catch (Driver.TransportException e)
        {
            return new CreateSessionResponse(e.Status);
        }
    }

    public async Task<DeleteSessionResponse> DeleteSession(string sessionId, DeleteSessionSettings? settings = null)
    {
        settings ??= new DeleteSessionSettings();
        var request = new DeleteSessionRequest
        {
            SessionId = sessionId
        };

        try
        {
            var response = await Driver.UnaryCall(
                method: QueryService.DeleteSessionMethod,
                request: request,
                settings: settings);


            return DeleteSessionResponse.FromProto(response.Data);
        }
        catch (Driver.TransportException e)
        {
            return new DeleteSessionResponse(e.Status);
        }
    }

    public SessionStateStream AttachSession(string sessionId, AttachSessionSettings? settings = null)
    {
        settings ??= new AttachSessionSettings();

        var request = new AttachSessionRequest { SessionId = sessionId };

        var streamIterator = Driver.StreamCall(
            method: QueryService.AttachSessionMethod,
            request: request,
            settings: settings
        );
        return new SessionStateStream(streamIterator);
    }

    public async Task<BeginTransactionResponse> BeginTransaction(
        string sessionId,
        Tx tx,
        BeginTransactionSettings? settings = null)
    {
        settings ??= new BeginTransactionSettings();

        var request = new BeginTransactionRequest { SessionId = sessionId, TxSettings = tx.ToProto().BeginTx };
        try
        {
            var response = await Driver.UnaryCall(
                QueryService.BeginTransactionMethod,
                request: request,
                settings: settings
            );
            return BeginTransactionResponse.FromProto(response.Data);
        }
        catch (Driver.TransportException e)
        {
            return new BeginTransactionResponse(e.Status);
        }
    }

    public async Task<CommitTransactionResponse> CommitTransaction(
        string sessionId,
        Tx tx,
        CommitTransactionSettings? settings = null)
    {
        settings ??= new CommitTransactionSettings();

        var request = new CommitTransactionRequest { SessionId = sessionId, TxId = tx.TxId };

        try
        {
            var response = await Driver.UnaryCall(
                QueryService.CommitTransactionMethod,
                request: request,
                settings: settings
            );
            return CommitTransactionResponse.FromProto(response.Data);
        }
        catch (Driver.TransportException e)
        {
            return new CommitTransactionResponse(e.Status);
        }
    }

    public async Task<RollbackTransactionResponse> RollbackTransaction(
        string sessionId,
        Tx tx,
        RollbackTransactionSettings? settings = null)
    {
        settings ??= new RollbackTransactionSettings();

        var request = new RollbackTransactionRequest { SessionId = sessionId, TxId = tx.TxId };
        try
        {
            var response = await Driver.UnaryCall(
                QueryService.RollbackTransactionMethod,
                request: request,
                settings: settings
            );
            return RollbackTransactionResponse.FromProto(response.Data);
        }
        catch (Driver.TransportException e)
        {
            return new RollbackTransactionResponse(e.Status);
        }
    }


    public ExecuteQueryStream ExecuteQuery(
        string sessionId,
        string queryString,
        Tx tx,
        IReadOnlyDictionary<string, YdbValue>? parameters,
        ExecuteQuerySettings? settings = null)
    {
        settings ??= new ExecuteQuerySettings();
        parameters ??= new Dictionary<string, YdbValue>();

        var request = new ExecuteQueryRequest
        {
            SessionId = sessionId,
            ExecMode = (Ydb.Query.ExecMode)settings.ExecMode,
            TxControl = tx.ToProto(),
            QueryContent = new QueryContent { Syntax = (Ydb.Query.Syntax)settings.Syntax, Text = queryString },
            StatsMode = (Ydb.Query.StatsMode)settings.StatsMode
        };

        request.Parameters.Add(parameters.ToDictionary(p => p.Key, p => p.Value.GetProto()));

        var streamIterator = Driver.StreamCall(
            method: QueryService.ExecuteQueryMethod,
            request: request,
            settings: settings);

        return new ExecuteQueryStream(streamIterator);
    }

    public async Task<IResponse> ExecOnSession(
        Func<Session, Task<IResponse>> func,
        RetrySettings? retrySettings = null
    )
    {
        if (_sessionPool is not SessionPool sessionPool)
        {
            throw new InvalidCastException(
                $"Unexpected cast error: {nameof(_sessionPool)} is not object of type {typeof(SessionPool).FullName}");
        }

        retrySettings ??= new RetrySettings();

        IResponse response = new ClientInternalErrorResponse("SessionRetry, unexpected response value.");
        Session? session = null;

        try
        {
            for (uint attempt = 0; attempt < retrySettings.MaxAttempts; attempt++)
            {
                if (session is null)
                {
                    var getSessionResponse = await sessionPool.GetSession();
                    if (getSessionResponse.Status.IsSuccess)
                    {
                        session = getSessionResponse.Session;
                    }

                    response = getSessionResponse;
                }

                if (session is not null)
                {
                    var funcResponse = await func(session);
                    if (funcResponse.Status.IsSuccess)
                    {
                        return funcResponse;
                    }

                    response = funcResponse;
                }

                var retryRule = retrySettings.GetRetryRule(response.Status.StatusCode);
                if (retryRule.DeleteSession)
                {
                    _logger.LogTrace($"Retry: Session ${session?.Id} invalid, disposing");
                    session?.Dispose();
                }
                else if (session is not null)
                {
                    sessionPool.ReturnSession(session);
                }

                if (retryRule.Idempotency == Idempotency.Idempotent && retrySettings.IsIdempotent ||
                    retryRule.Idempotency == Idempotency.NonIdempotent)
                {
                    _logger.LogTrace(
                        $"Retry: Session ${session?.Id}, " +
                        $"idempotent error {response.Status.StatusCode} retrying ");
                    await Task.Delay(retryRule.BackoffSettings.CalcBackoff(attempt));
                }
                else
                {
                    return response;
                }
            }
        }
        finally
        {
            session?.Dispose();
        }

        return response;
    }

    public async Task<QueryResponse<T>> Query<T>(string queryString, Dictionary<string, YdbValue> parameters,
        Func<ExecuteQueryStream, Task<T>> func, ITxModeSettings? txModeSettings = null,
        ExecuteQuerySettings? executeQuerySettings = null)
    {
        txModeSettings ??= new TxModeSerializableSettings();
        executeQuerySettings ??= new ExecuteQuerySettings();

        var response = await ExecOnSession(async session =>
        {
            var tx = Tx.Begin(txModeSettings);
            var stream = ExecuteQuery(session.Id, queryString, tx, parameters,
                executeQuerySettings);
            try
            {
                var response = await func(stream);
                return new QueryResponse<T>(new Status(StatusCode.Success), response);
            }
            catch (StatusUnsuccessfulException e)
            {
                return new QueryResponse<T>(e.Status);
            }
        });
        if (response is QueryResponse<T> queryResponse)
        {
            return queryResponse;
        }

        throw new InvalidCastException(
            $"Unexpected cast error: {nameof(response)} is not object of type {typeof(QueryResponse<T>).FullName}");
    }

    public async Task<QueryResponse<T>> Query<T>(string queryString, Func<ExecuteQueryStream, Task<T>> func,
        ITxModeSettings? txModeSettings = null, ExecuteQuerySettings? executeQuerySettings = null)
    {
        return await Query(queryString, new Dictionary<string, YdbValue>(), func, txModeSettings, executeQuerySettings);
    }

    // public async Task<T> ExecOnTx<T>(Func<Tx, T> func, ITxModeSettings? txModeSettings = null)
    // {
    //     throw new NotImplementedException();
    // }

    public void Dispose()
    {
        Dispose(true);
    }

    private void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }

        if (disposing)
        {
            _sessionPool.Dispose();
        }

        _disposed = true;
    }

    // public async Task SessionExecStream(
    //     Func<QuerySession, Task> operationFunc,
    //     RetrySettings? retrySettings = null)
    // {
    //     await Task.Delay(0);
    //     throw new NotImplementedException();
    // }
    //
    // public T SessionExecStream<T>(
    //     Func<QuerySession, T> func,
    //     RetrySettings? retrySettings = null)
    //     where T : IAsyncEnumerable<IResponse>, IAsyncEnumerator<IResponse>
    //
    // {
    //     throw new NotImplementedException();
    // }
    //
    // public async Task<T> SessionExec<T>(
    //     Func<QuerySession, Task<T>> func,
    //     RetrySettings? retrySettings = null)
    // {
    //     throw new NotImplementedException();
    // }
    //
    // public async Task<T> SessionExecTx<T>(Func<QuerySession, Tx, T> func)
    // {
    //     throw new NotImplementedException();
    // }
    //
    // public async Task<Tx> BeginTx()
    // {
    //     throw new NotImplementedException();
    // }
    //
    // public async Task<T> ExecTx<T>(Func<Tx, T> func, TxMode txMode = TxMode.SerializableRW, bool commit = false)
    // {
    //     throw new NotImplementedException();
    // }
}

public sealed class QueryResponse<TResult> : ResponseBase
{
    public TResult? Result;

    public QueryResponse(Status status, TResult? result = default) : base(status)
    {
        Result = result;
    }
}