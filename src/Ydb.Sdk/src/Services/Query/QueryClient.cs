using Microsoft.Extensions.Logging;
using Ydb.Sdk.Client;
using Ydb.Sdk.Services.Sessions;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query;

public class QueryClientConfig
{
    public SessionPoolConfig SessionPoolConfig { get; }

    public QueryClientConfig(
        SessionPoolConfig? sessionPoolConfig = null)
    {
        SessionPoolConfig = sessionPoolConfig ?? new SessionPoolConfig();
    }
}

public class QueryClient : QueryClientGrpc, IDisposable
{
    private readonly ISessionPool<Session> _sessionPool;
    private readonly ILogger _logger;
    private bool _disposed;

    public QueryClient(Driver driver, QueryClientConfig? config = null) : base(driver)
    {
        config ??= new QueryClientConfig();

        _logger = Driver.LoggerFactory.CreateLogger<QueryClient>();

        _sessionPool = new SessionPool(driver, config.SessionPoolConfig);
    }

    internal QueryClient(Driver driver, ISessionPool<Session> sessionPool) : base(driver)
    {
        _logger = driver.LoggerFactory.CreateLogger<QueryClient>();

        _sessionPool = sessionPool;
    }

    private async Task<IResponse> ExecOnSession(
        Func<Session, Task<IResponse>> func,
        RetrySettings? retrySettings = null
    )
    {
        if (_sessionPool is not SessionPool sessionPool)
        {
            throw new InvalidCastException(
                $"Unexpected cast error: {nameof(_sessionPool)} is not object of type {typeof(SessionPool).FullName}");
        }

        return await sessionPool.ExecOnSession(func, retrySettings);
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
        string queryString,
        Dictionary<string, YdbValue>? parameters,
        Func<ExecuteQueryStream, Task<T>> func,
        ITxModeSettings? txModeSettings = null,
        ExecuteQuerySettings? executeQuerySettings = null,
        RetrySettings? retrySettings = null)
    {
        parameters ??= new Dictionary<string, YdbValue>();
        txModeSettings ??= new TxModeSerializableSettings();
        executeQuerySettings ??= new ExecuteQuerySettings();

        var response = await ExecOnSession(
            async session =>
            {
                var tx = Tx.Begin(txModeSettings);
                tx.QueryClient = this;
                tx.SessionId = session.Id;
                return await tx.Query(queryString, parameters, func, executeQuerySettings);
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
        string queryString,
        Func<ExecuteQueryStream, Task<T>> func,
        ITxModeSettings? txModeSettings = null,
        ExecuteQuerySettings? executeQuerySettings = null,
        RetrySettings? retrySettings = null)
    {
        return await Query(queryString, new Dictionary<string, YdbValue>(), func, txModeSettings, executeQuerySettings,
            retrySettings);
    }

    public async Task<QueryResponse> Exec(string queryString,
        Dictionary<string, YdbValue>? parameters = null,
        ITxModeSettings? txModeSettings = null,
        ExecuteQuerySettings? executeQuerySettings = null,
        RetrySettings? retrySettings = null)
    {
        var response = await Query<None>(
            queryString,
            parameters,
            async session => await EmptyStreamReadFunc(session),
            txModeSettings,
            executeQuerySettings,
            retrySettings);
        return response;
    }


    internal static async Task<IReadOnlyList<Value.ResultSet.Row>> ReadAllRowsHelper(ExecuteQueryStream stream)
    {
        var rows = new List<Value.ResultSet.Row>();
        await foreach (var part in stream)
        {
            if (part.ResultSet is not null)
            {
                rows.AddRange(part.ResultSet.Rows);
            }
        }

        return rows;
    }

    public async Task<QueryResponseWithResult<Value.ResultSet.Row>> ReadFirstRow(string queryString,
        Dictionary<string, YdbValue>? parameters = null,
        ITxModeSettings? txModeSettings = null,
        ExecuteQuerySettings? executeQuerySettings = null,
        RetrySettings? retrySettings = null)
    {
        var response = await Query(queryString, parameters, async stream =>
            {
                var rows = await ReadAllRowsHelper(stream);
                return rows[0];
            }, txModeSettings,
            executeQuerySettings, retrySettings);
        return response;
    }

    public async Task<QueryResponseWithResult<IReadOnlyList<Value.ResultSet.Row>>> ReadAllRows(string queryString,
        Dictionary<string, YdbValue>? parameters = null,
        ITxModeSettings? txModeSettings = null,
        ExecuteQuerySettings? executeQuerySettings = null,
        RetrySettings? retrySettings = null)
    {
        var response = await Query(queryString, parameters, ReadAllRowsHelper, txModeSettings,
            executeQuerySettings, retrySettings);
        return response;
    }

    private async Task<QueryResponseWithResult<T>> Rollback<T>(Session session, Tx tx, Status status)
    {
        _logger.LogTrace($"Transaction {tx.TxId} not committed, try to rollback");
        try
        {
            var rollbackResponse = await RollbackTransaction(session.Id, tx);
            rollbackResponse.EnsureSuccess();
        }
        catch (StatusUnsuccessfulException e)
        {
            _logger.LogError($"Transaction {tx.TxId} rollback not successful {e.Status}");
            return new QueryResponseWithResult<T>(e.Status);
        }

        return new QueryResponseWithResult<T>(status);
    }

    public async Task<QueryResponseWithResult<T>> DoTx<T>(Func<Tx, Task<T>> func,
        ITxModeSettings? txModeSettings = null,
        RetrySettings? retrySettings = null)
    {
        var response = await ExecOnSession(
            async session =>
            {
                var beginTransactionResponse = await BeginTransaction(session.Id, Tx.Begin(txModeSettings));
                beginTransactionResponse.EnsureSuccess();
                var tx = beginTransactionResponse.Tx;
                tx.QueryClient = this;
                tx.SessionId = session.Id;

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

                var commitResponse = await CommitTransaction(session.Id, tx);
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
        ITxModeSettings? txModeSettings = null,
        RetrySettings? retrySettings = null)
    {
        var response = await DoTx<None>(
            async tx =>
            {
                await func(tx);
                return None.Instance;
            },
            txModeSettings,
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
