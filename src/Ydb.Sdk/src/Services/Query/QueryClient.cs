using Microsoft.Extensions.Logging;
using Ydb.Sdk.Client;
using Ydb.Sdk.Services.Sessions;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query;

public class QueryClientConfig
{
    public SessionPoolConfig SessionPoolConfig { get; set; } = new();
}

// Experimental
public class QueryClient : IDisposable
{
    private readonly SessionPool _sessionPool;
    private readonly QueryClientRpc _queryClientRpc;
    private readonly ILogger _logger;

    private bool _disposed;

    public QueryClient(Driver driver, QueryClientConfig? config = null)
    {
        config ??= new QueryClientConfig();

        _logger = driver.LoggerFactory.CreateLogger<QueryClient>();

        _sessionPool = new SessionPool(driver, config.SessionPoolConfig);
        _queryClientRpc = new QueryClientRpc(driver);
    }

    internal static async Task<None> EmptyStreamReadFunc(ExecuteQueryStream stream)
    {
        while (await stream.Next())
        {
            stream.Response.EnsureSuccess();
        }

        return None.Instance;
    }

    public async Task<QueryResponseWithResult<T>> Query<T>(
        string query,
        Dictionary<string, YdbValue>? parameters,
        Func<ExecuteQueryStream, Task<T>> func,
        TxMode? txMode = null,
        ExecuteQuerySettings? executeQuerySettings = null,
        RetrySettings? retrySettings = null)
    {
        parameters ??= new Dictionary<string, YdbValue>();
        executeQuerySettings ??= new ExecuteQuerySettings();

        var response = await _sessionPool.ExecOnSession(
            async session =>
            {
                var tx = Tx.Begin(txMode, _queryClientRpc, session.Id);
                return await tx.Query(query, parameters, func, executeQuerySettings);
            },
            retrySettings
        );
        return response switch
        {
            QueryResponseWithResult<T> queryResponseWithResult => queryResponseWithResult,
            _ => throw new InvalidCastException(
                $"Unexpected cast error: {nameof(response)} is not object of type {typeof(QueryResponseWithResult<T>).FullName}")
        };
    }

    public async Task<QueryResponseWithResult<T>> Query<T>(
        string query,
        Func<ExecuteQueryStream, Task<T>> func,
        TxMode? txMode = null,
        ExecuteQuerySettings? executeQuerySettings = null,
        RetrySettings? retrySettings = null)
    {
        return await Query(query, new Dictionary<string, YdbValue>(), func, txMode, executeQuerySettings,
            retrySettings);
    }

    public async Task<QueryResponse> Exec(
        string query,
        Dictionary<string, YdbValue>? parameters = null,
        TxMode? txMode = null,
        ExecuteQuerySettings? executeQuerySettings = null,
        RetrySettings? retrySettings = null)
    {
        return await Query<None>(
            query,
            parameters,
            async session => await EmptyStreamReadFunc(session),
            txMode,
            executeQuerySettings,
            retrySettings
        );
    }

    internal static async Task<IReadOnlyList<IReadOnlyList<Value.ResultSet.Row>>> ReadAllResultSetsHelper(
        ExecuteQueryStream stream)
    {
        var resultSets = new List<List<Value.ResultSet.Row>>();
        await foreach (var part in stream)
        {
            if (part.ResultSet is null) continue;
            while (resultSets.Count <= part.ResultSetIndex)
            {
                resultSets.Add(new List<Value.ResultSet.Row>());
            }

            resultSets[(int)part.ResultSetIndex].AddRange(part.ResultSet.Rows);
        }

        return resultSets;
    }

    internal static async Task<IReadOnlyList<Value.ResultSet.Row>> ReadAllRowsHelper(ExecuteQueryStream stream)
    {
        var resultSets = await ReadAllResultSetsHelper(stream);
        if (resultSets.Count > 1)
        {
            throw new QueryWrongResultFormatException("Should be only one resultSet");
        }

        return resultSets[0];
    }

    internal static async Task<Value.ResultSet.Row> ReadSingleRowHelper(ExecuteQueryStream stream)
    {
        Value.ResultSet.Row? row = null;
        await foreach (var part in stream)
        {
            if (row is null && part.ResultSet is null)
            {
                throw new QueryWrongResultFormatException("ResultSet is null");
            }

            if (part.ResultSet is not null)
            {
                if (row is not null || part.ResultSet.Rows.Count != 1)
                {
                    throw new QueryWrongResultFormatException("ResultSet should contain exactly one row");
                }

                row = part.ResultSet.Rows[0];
            }
        }

        return row!;
    }

    internal static async Task<YdbValue> ReadScalarHelper(ExecuteQueryStream stream)
    {
        var row = await ReadSingleRowHelper(stream);
        if (row.ColumnCount != 1)
        {
            throw new QueryWrongResultFormatException("Row should contain exactly one field");
        }

        return row[0];
    }

    public async Task<QueryResponseWithResult<IReadOnlyList<IReadOnlyList<Value.ResultSet.Row>>>> ReadAllResultSets(
        string query,
        Dictionary<string, YdbValue>? parameters = null,
        TxMode? txMode = null,
        ExecuteQuerySettings? executeQuerySettings = null,
        RetrySettings? retrySettings = null)
    {
        var response = await Query(query, parameters, ReadAllResultSetsHelper, txMode,
            executeQuerySettings, retrySettings);
        return response;
    }


    public async Task<QueryResponseWithResult<IReadOnlyList<Value.ResultSet.Row>>> ReadAllRows(
        string query,
        Dictionary<string, YdbValue>? parameters = null,
        TxMode? txMode = null,
        ExecuteQuerySettings? executeQuerySettings = null,
        RetrySettings? retrySettings = null)
    {
        var response = await Query(query, parameters, ReadAllRowsHelper, txMode,
            executeQuerySettings, retrySettings);
        return response;
    }


    public async Task<QueryResponseWithResult<Value.ResultSet.Row>> ReadSingleRow(
        string query,
        Dictionary<string, YdbValue>? parameters = null,
        TxMode? txMode = null,
        ExecuteQuerySettings? executeQuerySettings = null,
        RetrySettings? retrySettings = null)
    {
        return await Query(query, parameters, ReadSingleRowHelper, txMode, executeQuerySettings, retrySettings);
    }

    public async Task<QueryResponseWithResult<YdbValue>> ReadScalar(
        string query,
        Dictionary<string, YdbValue>? parameters = null,
        TxMode? txMode = null,
        ExecuteQuerySettings? executeQuerySettings = null,
        RetrySettings? retrySettings = null)
    {
        return await Query(query, parameters, ReadScalarHelper, txMode, executeQuerySettings, retrySettings);
    }

    private async Task<QueryResponseWithResult<T>> Rollback<T>(Session session, Tx tx, Status status)
    {
        _logger.LogTrace($"Transaction {tx.TxId} not committed, try to rollback");
        try
        {
            var rollbackResponse = await _queryClientRpc.RollbackTransaction(session.Id, tx);
            rollbackResponse.EnsureSuccess();
        }
        catch (StatusUnsuccessfulException e)
        {
            _logger.LogError($"Transaction {tx.TxId} rollback not successful {e.Status}");
            return new QueryResponseWithResult<T>(e.Status);
        }

        return new QueryResponseWithResult<T>(status);
    }

    public async Task<QueryResponseWithResult<T>> DoTx<T>(
        Func<Tx, Task<T>> func,
        TxMode? txMode = null,
        RetrySettings? retrySettings = null)
    {
        var response = await _sessionPool.ExecOnSession(
            async session =>
            {
                var tx = Tx.Begin(txMode, _queryClientRpc, session.Id);
                
                var beginTransactionResponse = await _queryClientRpc
                    .BeginTransaction(session.Id, tx);
                beginTransactionResponse.EnsureSuccess();
                tx.TxId = beginTransactionResponse.TxId!;

                T response;
                try
                {
                    response = await func(tx);
                }
                catch (StatusUnsuccessfulException e)
                {
                    var rollbackResponse = await Rollback<T>(session, tx, e.Status);
                    return rollbackResponse;
                }
                catch (Exception e)
                {
                    var status = new Status(
                        StatusCode.ClientInternalError,
                        $"Failed to execute lambda on tx {tx.TxId}: {e.Message}");
                    var rollbackResponse = await Rollback<T>(session, tx, status);
                    return rollbackResponse;
                }

                var commitResponse = await _queryClientRpc.CommitTransaction(session.Id, tx);
                if (!commitResponse.Status.IsSuccess)
                {
                    var rollbackResponse = await Rollback<T>(session, tx, commitResponse.Status);
                    return rollbackResponse;
                }

                return response switch
                {
                    None => new QueryResponseWithResult<T>(Status.Success),
                    _ => new QueryResponseWithResult<T>(Status.Success, response)
                };
            },
            retrySettings
        );
        return response switch
        {
            QueryResponseWithResult<T> queryResponseWithResult => queryResponseWithResult,
            _ => throw new InvalidCastException(
                $"Unexpected cast error: {nameof(response)} is not object of type {typeof(QueryResponseWithResult<T>).FullName}")
        };
    }

    public async Task<QueryResponse> DoTx(Func<Tx, Task> func,
        TxMode? txMode = null,
        RetrySettings? retrySettings = null)
    {
        var response = await DoTx<None>(
            async tx =>
            {
                await func(tx);
                return None.Instance;
            },
            txMode,
            retrySettings
        );
        return response;
    }

    internal record None
    {
        internal static readonly None Instance = new();
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    private void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }

        if (disposing)
        {
            _sessionPool.Dispose();
        }

        _disposed = true;
    }
}

public class QueryResponse : ResponseBase
{
    public QueryResponse(Status status) : base(status)
    {
    }
}

public sealed class QueryResponseWithResult<TResult> : QueryResponse
{
    public readonly TResult? Result;

    public QueryResponseWithResult(Status status, TResult? result = default) : base(status)
    {
        Result = result;
    }
}

public class QueryWrongResultFormatException : Exception
{
    public QueryWrongResultFormatException(string message) : base(message)
    {
    }
}
