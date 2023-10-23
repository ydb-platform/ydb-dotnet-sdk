using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query;

public class CommitTxResponse
{
    public void EnsureSuccess()
    {
        throw new NotImplementedException();
    }
}

public class RollbackTxResponse
{
}

public class Tx : IDisposable, IAsyncDisposable
{
    public object TxSettings; 
    
    private Tx()
    {
        throw new NotImplementedException();
    }
    
    public static Tx Begin() 
    {
        throw new NotImplementedException();
    }

    public Tx WithCommit()
    {
        return this;
    }

    // calls  rpc CommitTransaction(Query.CommitTransactionRequest) returns (Query.CommitTransactionResponse);
    public CommitTxResponse Commit()
    {
        throw new NotImplementedException();
    }

    // calls  rpc RollbackTransaction(Query.RollbackTransactionRequest) returns (Query.RollbackTransactionResponse);
    public RollbackTxResponse Rollback()
    {
        throw new NotImplementedException();
    }

    public void Dispose()
    {
        throw new NotImplementedException();
    }

    public ValueTask DisposeAsync()
    {
        throw new NotImplementedException();
    }


    public ExecuteQueryStream ExecuteQueryYql(
        string query,
        IReadOnlyDictionary<string, YdbValue>? parameters = null,
        ExecuteQuerySettings? settings = null)
    {
        throw new NotImplementedException();
    }

    public async Task<ExecuteQueryStream> Query(string query, Dictionary<string,YdbValue> parameters)
    {
        throw new NotImplementedException();
    }
}