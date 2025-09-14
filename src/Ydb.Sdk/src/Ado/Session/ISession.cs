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
        TransactionControl? txControl
    );

    Task CommitTransaction(string txId, CancellationToken cancellationToken = default);

    Task RollbackTransaction(string txId, CancellationToken cancellationToken = default);

    void OnNotSuccessStatusCode(StatusCode code);
}
