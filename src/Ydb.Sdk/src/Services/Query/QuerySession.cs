using Ydb.Sdk.Client;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query;

public enum QueryExecMode
{
    Unspecified = 0,
    Parse = 10,
    Validate = 20,
    Explain = 30,

    // reserved 40; // EXEC_MODE_PREPARE
    Execute = 50
}

public enum QuerySyntax
{
    Unspecified = 0,
    YqlV1 = 1, // YQL
    Pg = 2 // PostgresQL
}

public class ExecuteQuerySettings : RequestSettings
{
}

public class ExecuteQueryPart : ResponseWithResultBase<ExecuteQueryPart.ResultData>
{
    protected ExecuteQueryPart(Status status, ResultData? result = null) : base(status, result)
    {
    }

    public class ResultData
    {
        public Value.ResultSet? ResultSet => throw new NotImplementedException();
    }
}

public class ExecuteQueryResponse : IResponse
{
    // Заглушка, надо вытянуть из Ydb.Protos
    public Status Status => throw new NotImplementedException();
}

public class ExecuteQueryStream : StreamResponse<ExecuteQueryResponse, ExecuteQueryPart>,
    IAsyncEnumerable<ExecuteQueryPart>, IAsyncEnumerator<ExecuteQueryPart>
{
    public object ExecStats { get; }

    internal ExecuteQueryStream(Driver.StreamIterator<ExecuteQueryResponse> iterator) : base(iterator)
    {
        throw new NotImplementedException();
    }

    protected override ExecuteQueryPart MakeResponse(ExecuteQueryResponse protoResponse)
    {
        throw new NotImplementedException();
    }

    protected override ExecuteQueryPart MakeResponse(Status status)
    {
        throw new NotImplementedException();
    }

    public IAsyncEnumerator<ExecuteQueryPart> GetAsyncEnumerator(
        CancellationToken cancellationToken = new CancellationToken())
    {
        throw new NotImplementedException();
    }

    public ValueTask DisposeAsync()
    {
        throw new NotImplementedException();
    }

    public ValueTask<bool> MoveNextAsync()
    {
        throw new NotImplementedException();
    }

    public ExecuteQueryPart Current { get; }
}

public class QuerySession
{
    public ExecuteQueryStream ExecuteQuery(
        string query,
        Tx tx,
        IReadOnlyDictionary<string, YdbValue>? parameters,
        QuerySyntax syntax,
        ExecuteQuerySettings? settings = null)
    {
        // settings ??= new ExecuteQuerySettings();
        // parameters ??= new Dictionary<string, YdbValue>();

        // var request = new ExecuteQueryRequest
        // {
        //     SessionId = Id,
        //     ...
        // };

        // var streamIterator = Driver.StreamCall(
        //     method: QueryService.ExecuteQuery,
        //     request: request,
        //     settings: settings);
        //
        // return new ExecuteQueryStream(streamIterator);

        throw new NotImplementedException();
    }


    public ExecuteQueryStream ExecuteQueryYql(
        string query,
        Tx tx,
        IReadOnlyDictionary<string, YdbValue>? parameters = null,
        ExecuteQuerySettings? settings = null)
    {
        throw new NotImplementedException();
    }

    public async Task<T> TxExec<T>(Func<Tx, Task<T>> func)
    {
        var tx = BeginTx();
        try
        {
            await func(tx);
            throw new CommitException();
        }
        catch (CommitException)
        {
            try
            {

            }
            catch (Exception e)
            {
                try
                {
                    tx.Rollback();
                }
                catch (RollbackException exception)
                {
                    Console.WriteLine(exception);
                    throw;
                }
                Console.WriteLine(e);
                throw;
            }
        }
        throw new NotImplementedException();
    }

    // calls  rpc BeginTransaction(Query.BeginTransactionRequest) returns (Query.BeginTransactionResponse);
    public Tx BeginTx()
    {
        throw new NotImplementedException();
    }
}


public class CommitException : Exception
{
}
public class RollbackException : Exception
{
}