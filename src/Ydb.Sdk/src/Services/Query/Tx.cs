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

public class Tx : IDisposable
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
}