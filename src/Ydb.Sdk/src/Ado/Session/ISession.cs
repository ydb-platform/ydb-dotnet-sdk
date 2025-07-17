using Ydb.Query;
using Ydb.Sdk.Value;
using TransactionControl = Ydb.Query.TransactionControl;

namespace Ydb.Sdk.Ado.Session;

internal interface ISession
{
    bool IsBroken { get; }

    ValueTask<IServerStream<ExecuteQueryResponsePart>> ExecuteQuery(
        string query,
        Dictionary<string, YdbValue> parameters,
        GrpcRequestSettings settings,
        TransactionControl? txControl
    );

    Task CommitTransaction(string txId, CancellationToken cancellationToken = default);

    Task RollbackTransaction(string txId, CancellationToken cancellationToken = default);

    void OnNotSuccessStatusCode(StatusCode code);

    void Close();
}
