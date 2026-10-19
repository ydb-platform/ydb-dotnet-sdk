using Ydb.Query;
using Ydb.Query.V1;

namespace Ydb.Sdk.Ado.Session;

internal class ImplicitSession(IDriver driver, ImplicitSessionSource source) : ISession
{
    public IDriver Driver { get; } = driver;

    public bool IsBroken => false;

    public ValueTask<IServerStream<ExecuteQueryResponsePart>> ExecuteQuery(
        string query,
        Dictionary<string, TypedValue> parameters,
        GrpcRequestSettings settings,
        TransactionControl? txControl,
        StatsMode statsMode = StatsMode.None
    )
    {
        var request = new ExecuteQueryRequest
        {
            ExecMode = ExecMode.Execute,
            QueryContent = new QueryContent { Text = query, Syntax = Syntax.YqlV1 },
            StatsMode = statsMode.ToProto(),
            TxControl = txControl
        };
        request.Parameters.Add(parameters);

        return Driver.ServerStreamCall(QueryService.ExecuteQueryMethod, request, settings);
    }

    public void OnNotSuccessStatusCode(StatusCode code)
    {
    }

    public void Dispose() => source.ReleaseLease();
}
