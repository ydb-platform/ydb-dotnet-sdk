using Ydb.Query;
using TransactionControl = Ydb.Query.TransactionControl;

namespace Ydb.Sdk.Ado.Session;

internal interface ISession : IDisposable
{
    IDriver Driver { get; }

    bool IsBroken { get; }

    ValueTask<IServerStream<ExecuteQueryResponsePart>> ExecuteQuery(
        string query,
        Dictionary<string, TypedValue> parameters,
        GrpcRequestSettings settings,
        TransactionControl? txControl,
        StatsMode statsMode = StatsMode.None
    );

    void OnNotSuccessStatusCode(StatusCode code);
}
