using Microsoft.Extensions.Logging;
using Ydb.Sdk.Client;

namespace Ydb.Sdk.Services.Query;

/// <summary>
/// Sessions are basic primitives for communicating with YDB Query Service. The are similar to
/// connections for classic relational DBs. Sessions serve three main purposes:
/// 1. Provide a flow control for DB requests with limited number of active channels.
/// 2. Distribute load evenly across multiple DB nodes.
/// 3. Store state for volatile stateful operations, such as short-living transactions.
/// </summary>
public class Session : ClientBase
{
    private readonly ILogger _logger;

    internal Session(Driver driver, SessionPool? sessionPool, string id, long nodeId, string? endpoint)
        : base(driver)
    {
        // _sessionPool = sessionPool;
        _logger = Driver.LoggerFactory.CreateLogger<Session>();
        Id = id;
        NodeId = nodeId;
        Endpoint = endpoint;
    }


    public string Id { get; }
    public long NodeId { get; }

    internal string? Endpoint { get; }

    //
    //
    // public ExecuteQueryStream ExecuteQueryYql(
    //     string query,
    //     Tx tx,
    //     IReadOnlyDictionary<string, YdbValue>? parameters = null,
    //     ExecuteQuerySettings? settings = null)
    // {
    //     throw new NotImplementedException();
    // }
    //
    // public async Task<T> TxExec<T>(Func<Tx, Task<T>> func)
    // {
    //     var tx = BeginTx();
    //     try
    //     {
    //         await func(tx);
    //         throw new CommitException();
    //     }
    //     catch (CommitException)
    //     {
    //         try
    //         {
    //
    //         }
    //         catch (Exception e)
    //         {
    //             try
    //             {
    //                 tx.Rollback();
    //             }
    //             catch (RollbackException exception)
    //             {
    //                 Console.WriteLine(exception);
    //                 throw;
    //             }
    //             Console.WriteLine(e);
    //             throw;
    //         }
    //     }
    //     throw new NotImplementedException();
    // }

    // calls  rpc BeginTransaction(Query.BeginTransactionRequest) returns (Query.BeginTransactionResponse);
    // public Tx BeginTx()
    // {
    //     throw new NotImplementedException();
    // }
}

public class CommitException : Exception
{
}

public class RollbackException : Exception
{
}