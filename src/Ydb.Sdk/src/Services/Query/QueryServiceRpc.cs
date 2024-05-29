using Ydb.Query;
using Ydb.Query.V1;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query;

internal class QueryServiceRpc : IAsyncDisposable
{
    private static readonly CreateSessionRequest CreateSessionRequest = new();
    private static readonly DeleteSessionRequest DeleteSessionRequest = new();

    private readonly Driver _driver;

    public QueryServiceRpc(Driver driver)
    {
        _driver = driver;
    }

    internal Task<CreateSessionResponse> CreateSession(GrpcRequestSettings grpcRequestSettings)
    {
        return _driver.UnaryCall(QueryService.CreateSessionMethod, CreateSessionRequest, grpcRequestSettings);
    }

    internal Task<DeleteSessionResponse> DeleteSession(GrpcRequestSettings grpcRequestSettings)
    {
        return _driver.UnaryCall(QueryService.DeleteSessionMethod, DeleteSessionRequest, grpcRequestSettings);
    }

    internal Driver.StreamIterator<SessionState> AttachSession(string sessionId,
        GrpcRequestSettings grpcRequestSettings)
    {
        return _driver.StreamCall(QueryService.AttachSessionMethod, new AttachSessionRequest
            { SessionId = sessionId }, grpcRequestSettings);
    }

    internal IAsyncEnumerable<ExecuteQueryResponsePart> ExecuteQuery(string query, string sessionId, TxMode txMode,
        IReadOnlyDictionary<string, YdbValue> parameters, ExecuteQuerySettings settings)
    {
        ExecuteQueryRequest request = new()
        {
            SessionId = sessionId,
            ExecMode = (Ydb.Query.ExecMode)settings.ExecMode,
            QueryContent = new QueryContent { Text = query, Syntax = (Ydb.Query.Syntax)settings.Syntax },
            StatsMode = (Ydb.Query.StatsMode)settings.StatsMode,
            ConcurrentResultSets = settings.ConcurrentResultSets
        };

        if (txMode != TxMode.None)
        {
            request.TxControl = new TransactionControl
                { BeginTx = txMode.TransactionSettings(), CommitTx = settings.AutoCommit };
        }

        request.Parameters.Add(parameters.ToDictionary(p => p.Key, p => p.Value.GetProto()));

        return _driver.StreamCall(QueryService.ExecuteQueryMethod, request, settings);
    }

    internal Task<BeginTransactionResponse> BeginTransaction(string sessionId, TxMode txMode,
        GrpcRequestSettings settings)
    {
        return _driver.UnaryCall(QueryService.BeginTransactionMethod, new BeginTransactionRequest
            { SessionId = sessionId, TxSettings = txMode.TransactionSettings() }, settings);
    }

    internal Task<CommitTransactionResponse> CommitTransaction(string sessionId, string txId,
        GrpcRequestSettings settings)
    {
        return _driver.UnaryCall(QueryService.CommitTransactionMethod, new CommitTransactionRequest
            { SessionId = sessionId, TxId = txId }, settings);
    }

    internal Task<RollbackTransactionResponse> RollbackTransaction(string sessionId, string txId,
        GrpcRequestSettings settings)
    {
        return _driver.UnaryCall(QueryService.RollbackTransactionMethod, new RollbackTransactionRequest
            { SessionId = sessionId, TxId = txId }, settings);
    }

    public ValueTask DisposeAsync()
    {
        return _driver.DisposeAsync();
    }
}
