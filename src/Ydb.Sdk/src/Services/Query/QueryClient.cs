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

public class RetrySettings
{
    public bool IsIdempotent;
}

// public class ExecuteQuerySettings : RequestSettings
// {
// }

public class ExecuteQueryPart : ResponseWithResultBase<ExecuteQueryPart.ResultData>
{
    protected ExecuteQueryPart(Status status, ResultData? result = null) : base(status, result)
    {
    }

    public class ResultData
    {
        public Value.ResultSet? ResultSet => throw new NotImplementedException();
    }
}

public class ExecuteQueryStream : StreamResponse<ExecuteQueryResponse, ExecuteQueryPart>,
    IAsyncEnumerable<ExecuteQueryPart>, IAsyncEnumerator<ExecuteQueryPart>
{
    public object ExecStats { get; }

    internal ExecuteQueryStream(Driver.StreamIterator<ExecuteQueryResponse> iterator) : base(iterator)
    {
        throw new NotImplementedException();
    }

    protected override ExecuteQueryPart MakeResponse(ExecuteQueryResponse protoResponse)
    {
        throw new NotImplementedException();
    }

    protected override ExecuteQueryPart MakeResponse(Status status)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerator<ExecuteQueryPart> GetAsyncEnumerator(
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

    public ExecuteQueryPart Current { get; }
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

    public static CreateSessionResponse FromProto(Ydb.Query.CreateSessionResponse proto, Driver driver,
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

    public static DeleteSessionResponse FromProto(Ydb.Query.DeleteSessionResponse proto)
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

    public static SessionState FromProto(Ydb.Query.SessionState proto)
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

    internal async Task BeginTransaction()
    {
    }

    internal async Task CommitTransaction()
    {
    }

    internal async Task RollbackTransaction()
    {
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