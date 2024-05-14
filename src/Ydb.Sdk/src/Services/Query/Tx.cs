using Ydb.Query;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query;

public enum TxMode
{
    None,

    SerializableRw,
    SnapshotRo,
    StaleRo,

    OnlineRo,
    OnlineInconsistentRo
}

internal static class TxModeExtensions
{
    private static readonly TransactionSettings SerializableRw = new()
        { SerializableReadWrite = new SerializableModeSettings() };

    private static readonly TransactionSettings SnapshotRo = new()
        { SnapshotReadOnly = new SnapshotModeSettings() };

    private static readonly TransactionSettings StaleRo = new()
        { StaleReadOnly = new StaleModeSettings() };

    private static readonly TransactionSettings OnlineRo = new()
        { OnlineReadOnly = new OnlineModeSettings { AllowInconsistentReads = false } };

    private static readonly TransactionSettings OnlineInconsistentRo = new()
        { OnlineReadOnly = new OnlineModeSettings { AllowInconsistentReads = true } };

    internal static TransactionSettings? TransactionSettings(this TxMode mode)
    {
        return mode switch
        {
            TxMode.SerializableRw => SerializableRw,
            TxMode.SnapshotRo => SnapshotRo,
            TxMode.StaleRo => StaleRo,
            TxMode.OnlineRo => OnlineRo,
            TxMode.OnlineInconsistentRo => OnlineInconsistentRo,
            _ => null
        };
    }
}

public class Tx
{
    private QueryClientRpc Client { get; }
    private string SessionId { get; }

    internal string? TxId { get; set; }
    internal TxMode TxMode { get; }
    internal bool AutoCommit { get; }

    private Tx(TxMode txMode, QueryClientRpc client, string sessionId, bool commit)
    {
        TxMode = txMode;
        Client = client;
        SessionId = sessionId;
        AutoCommit = commit;
    }

    internal static Tx Begin(TxMode? txMode, QueryClientRpc client, string sessionId, bool commit = true)
    {
        return new Tx(txMode ?? TxMode.SerializableRw, client, sessionId, commit);
    }

    public async Task<QueryResponseWithResult<T>> Query<T>(
        string query,
        Dictionary<string, YdbValue>? parameters,
        Func<ExecuteQueryStream, Task<T>> func,
        ExecuteQuerySettings? executeQuerySettings = null)
    {
        var stream = Client.ExecuteQuery(SessionId, query, this, parameters, executeQuerySettings);

        try
        {
            var response = await func(stream);
            return response is QueryClient.None
                ? new QueryResponseWithResult<T>(Status.Success)
                : new QueryResponseWithResult<T>(Status.Success, response);
        }
        catch (StatusUnsuccessfulException e)
        {
            return new QueryResponseWithResult<T>(e.Status);
        }
    }

    public async Task<QueryResponse> Exec(
        string query,
        Dictionary<string, YdbValue>? parameters = null,
        ExecuteQuerySettings? executeQuerySettings = null)
    {
        var response = await Query<QueryClient.None>(
            query,
            parameters,
            async session =>
            {
                await QueryClient.EmptyStreamReadFunc(session);
                return QueryClient.None.Instance;
            },
            executeQuerySettings);
        return response;
    }

    public async Task<QueryResponseWithResult<Value.ResultSet.Row>> ReadSingleRow(
        string query,
        Dictionary<string, YdbValue>? parameters = null,
        ExecuteQuerySettings? executeQuerySettings = null)
    {
        return await Query(query, parameters, QueryClient.ReadSingleRowHelper, executeQuerySettings);
    }

    public async Task<QueryResponseWithResult<YdbValue>> ReadScalar(
        string query,
        Dictionary<string, YdbValue>? parameters = null,
        ExecuteQuerySettings? executeQuerySettings = null)
    {
        return await Query(query, parameters, QueryClient.ReadScalarHelper, executeQuerySettings);
    }
}
