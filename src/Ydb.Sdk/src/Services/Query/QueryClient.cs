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
    public ExecMode ExecMode { get; set; }
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

public class ExecuteQueryResponsePart : ResponseBase
{
    internal ExecuteQueryResponsePart(Status status) : base(status)
    {
    }

    private ExecuteQueryResponsePart(Ydb.Query.ExecuteQueryResponsePart proto)
        : base(Status.FromProto(proto.Status, proto.Issues))
    {
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

public class QueryClient :
    ClientBase,
    IDisposable
{
    public QueryClient(Driver driver) : base(driver)
    {
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


    public async Task<T> Query<T>(string query, Dictionary<string, YdbValue> parameters,
        Func<ExecuteQueryStream, T> func, TxMode txMode = TxMode.SerializableRW)
    {
        throw new NotImplementedException();
    }

    public async Task<T> Tx<T>(Func<Tx, T> func, TxMode txMode = TxMode.SerializableRW)
    {
        throw new NotImplementedException();
    }

    public void Dispose()
    {
    }

    public async Task ExecOnSession<T>(
        Func<Session, T> func,
        RetrySettings? retrySettings = null
    )
    {
        retrySettings = new RetrySettings();
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