using Ydb.Query;
using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.Session;

internal interface IAdoSession
{
    internal ValueTask<IServerStream<ExecuteQueryResponsePart>> ExecuteQuery(
        string query,
        Dictionary<string, YdbValue>? parameters,
        ExecuteQuerySettings? settings,
        TransactionControl? txControl
    );
    
    
}
