using Ydb.Query;
using Ydb.Query.V1;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.Session;

internal class ImplicitSession : ISession
{
    private readonly IDriver _driver;

    public ImplicitSession(IDriver driver)
    {
        _driver = driver;
    }

    public bool IsBroken => false;

    public ValueTask<IServerStream<ExecuteQueryResponsePart>> ExecuteQuery(
        string query,
        Dictionary<string, YdbValue> parameters,
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
        request.Parameters.Add(parameters.ToDictionary(p => p.Key, p => p.Value.GetProto()));

        return _driver.ServerStreamCall(QueryService.ExecuteQueryMethod, request, settings);
    }

    public Task CommitTransaction(string txId, CancellationToken cancellationToken = default) =>
        throw NotSupportedTransaction;

    public Task RollbackTransaction(string txId, CancellationToken cancellationToken = default) =>
        throw NotSupportedTransaction;

    public void OnNotSuccessStatusCode(StatusCode code)
    {
    }

    public void Close()
    {
    }

    private static YdbException NotSupportedTransaction =>
        new(StatusCode.BadRequest, "Transactions are not supported in implicit sessions");
}
