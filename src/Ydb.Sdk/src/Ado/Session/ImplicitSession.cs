using Ydb.Query;
using Ydb.Query.V1;

namespace Ydb.Sdk.Ado.Session;

internal class ImplicitSession : ISession
{
    public ImplicitSession(IDriver driver)
    {
        Driver = driver;
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

    public Task CommitTransaction(string txId, CancellationToken cancellationToken = default) =>
        throw NotSupportedTransaction;

    public Task RollbackTransaction(string txId, CancellationToken cancellationToken = default) =>
        throw NotSupportedTransaction;

    public void OnNotSuccessStatusCode(StatusCode code)
    {
    }

    public void Dispose()
    {
    }

    private static YdbException NotSupportedTransaction => new("Transactions are not supported in implicit sessions");
}
