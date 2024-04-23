using Ydb.Query;
using Ydb.Sdk.Client;

namespace Ydb.Sdk.Services.Query;

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
    /// PostgresSQL
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

internal class CreateSessionSettings : RequestSettings
{
}

internal class DeleteSessionSettings : RequestSettings
{
}

internal class AttachSessionSettings : RequestSettings
{
}

internal class BeginTransactionSettings : RequestSettings
{
}

internal class CommitTransactionSettings : RequestSettings
{
}

internal class RollbackTransactionSettings : RequestSettings
{
}

internal class CreateSessionResponse : ResponseWithResultBase<CreateSessionResponse.ResultData>
{
    internal CreateSessionResponse(Status status, ResultData? result = null)
        : base(status, result)
    {
    }

    public class ResultData
    {
        private ResultData(Session session)
        {
            Session = session;
        }

        public Session Session { get; }

        internal static ResultData FromProto(Ydb.Query.CreateSessionResponse resultProto, Driver driver,
            string endpoint)
        {
            var session = new Session(
                driver: driver,
                sessionPool: null,
                id: resultProto.SessionId,
                nodeId: resultProto.NodeId,
                endpoint: endpoint);

            return new ResultData(
                session: session
            );
        }
    }
}

internal class DeleteSessionResponse : ResponseBase
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

internal class SessionState : ResponseBase
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

internal class SessionStateStream : StreamResponse<Ydb.Query.SessionState, SessionState>
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
    public readonly long ResultSetIndex;
    public readonly Value.ResultSet? ResultSet;

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
    , IAsyncEnumerable<ExecuteQueryResponsePart>
{
    internal ExecuteQueryStream(Driver.StreamIterator<Ydb.Query.ExecuteQueryResponsePart> iterator) : base(iterator)
    {
    }

    public new async Task<bool> Next()
    {
        var isNext = await base.Next();
        if (isNext)
        {
            Response.EnsureSuccess();
        }

        return isNext;
    }

    protected override ExecuteQueryResponsePart MakeResponse(Ydb.Query.ExecuteQueryResponsePart protoResponse)
    {
        return ExecuteQueryResponsePart.FromProto(protoResponse);
    }

    protected override ExecuteQueryResponsePart MakeResponse(Status status)
    {
        return new ExecuteQueryResponsePart(status);
    }

    public async IAsyncEnumerator<ExecuteQueryResponsePart> GetAsyncEnumerator(
        CancellationToken cancellationToken = new())
    {
        while (await Next())
        {
            yield return Response;
        }
    }
}

internal class BeginTransactionResponse : ResponseBase
{
    internal BeginTransactionResponse(Status status) : base(status)
    {
    }

    internal Tx? Tx { get; }

    private BeginTransactionResponse(Ydb.Query.BeginTransactionResponse proto, QueryClientGrpc client, string sessionId)
        : base(
            Status.FromProto(proto.Status, proto.Issues))
    {
        var txId = proto.TxMeta.Id;
        Tx = new Tx(new TransactionControl { TxId = txId }, client, sessionId);
    }

    internal static BeginTransactionResponse FromProto(Ydb.Query.BeginTransactionResponse proto, QueryClientGrpc client,
        string sessionId)
    {
        return new BeginTransactionResponse(proto, client, sessionId);
    }
}

internal class CommitTransactionResponse : ResponseBase
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

internal class RollbackTransactionResponse : ResponseBase
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
