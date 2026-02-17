using System.Diagnostics;
using Ydb.Query;
using Ydb.Query.V1;

namespace Ydb.Sdk.Ado.Session;

internal class ImplicitSession : ISession
{
    private readonly ImplicitSessionSource _source;

    public ImplicitSession(IDriver driver, ImplicitSessionSource source)
    {
        Driver = driver;
        _source = source;
    }

    public IDriver Driver { get; }
    public bool IsBroken => false;

    public ValueTask<IServerStream<ExecuteQueryResponsePart>> ExecuteQuery(
        string query,
        Dictionary<string, TypedValue> parameters,
        GrpcRequestSettings settings,
        TransactionControl? txControl
    )
    {
        if (txControl is not null && !txControl.CommitTx)
        {
            throw NotSupportedTransaction;
        }

        var request = new ExecuteQueryRequest
        {
            ExecMode = ExecMode.Execute,
            QueryContent = new QueryContent { Text = query, Syntax = Syntax.YqlV1 },
            StatsMode = StatsMode.None,
            TxControl = txControl
        };
        request.Parameters.Add(parameters);

        return Driver.ServerStreamCall(QueryService.ExecuteQueryMethod, request, settings);
    }

    public Task CommitTransaction(string txId, Activity? dbActivity = null, CancellationToken cancellationToken = default) =>
        throw NotSupportedTransaction;

    public Task RollbackTransaction(string txId, Activity? dbActivity = null, CancellationToken cancellationToken = default) =>
        throw NotSupportedTransaction;

    public void OnNotSuccessStatusCode(StatusCode code)
    {
    }

    public void Dispose() => _source.ReleaseLease();

    private static YdbException NotSupportedTransaction => new("Transactions are not supported in implicit session");
}
