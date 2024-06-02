using System.Collections.Immutable;
using Ydb.Query;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query;

public class QueryClientConfig
{
    public int SizeSessionPool { get; set; } = 100;
}

public class QueryClient : IAsyncDisposable
{
    private readonly SessionPool _sessionPool;

    public QueryClient(Driver driver, QueryClientConfig? config = null)
    {
        config ??= new QueryClientConfig();

        _sessionPool = new SessionPool(driver, config.SizeSessionPool);
    }

    // Reading the result set stream into memory
    public Task<Result<IReadOnlyList<Value.ResultSet.Row>>> ReadAllRows(string query,
        Dictionary<string, YdbValue>? parameters = null, TxMode txMode = TxMode.Unspecified,
        ExecuteQuerySettings? settings = null)
    {
        return _sessionPool.ExecOnSession<IReadOnlyList<Value.ResultSet.Row>>(async session =>
        {
            List<Value.ResultSet.Row> rows = new();

            await foreach (var part in session.ExecuteQuery(query, parameters, settings, txMode.TransactionControl()))
            {
                if (part.Status.IsNotSuccess)
                {
                    return Result.Fail<IReadOnlyList<Value.ResultSet.Row>>(part.Status);
                }

                rows.AddRange(part.ResultSet?.FromProto().Rows ?? ImmutableList<Value.ResultSet.Row>.Empty);
            }

            return Result.Success<IReadOnlyList<Value.ResultSet.Row>>(rows.AsReadOnly());
        });
    }

    public async Task<(Status, Value.ResultSet.Row?)> ReadRow(string query,
        Dictionary<string, YdbValue>? parameters = null, TxMode txMode = TxMode.Unspecified,
        ExecuteQuerySettings? settings = null)
    {
        var result = await ReadAllRows(query, parameters, txMode, settings);

        return result is { IsSuccess: true, Value.Count: > 0 }
            ? (result.Status, result.Value[0])
            : (result.Status, null);
    }

    public async Task<Status> Exec(string query, Dictionary<string, YdbValue>? parameters = null,
        TxMode txMode = TxMode.Unspecified, ExecuteQuerySettings? settings = null)
    {
        var result = await ReadAllRows(query, parameters, txMode, settings);

        return result.Status;
    }

    public Task<Result<T>> DoTx<T>(Func<QueryTx, Result<T>> queryTx, TxMode txMode = TxMode.SerializableRw)
        where T : class
    {
        return _sessionPool.ExecOnSession<T>(async session =>
        {
            var tx = new QueryTx(session, txMode);

            try
            {
                var result = queryTx(tx);

                var commitResult = await tx.Commit();
                if (result.IsSuccess && commitResult.IsNotSuccess)
                {
                    result = Result.Fail<T>(commitResult); // not success Commited
                }

                return result;
            }
            catch (UnexpectedResultException e)
            {
                return Result.Fail<T>(e.Status);
            }
            finally
            {
                session.Release();
            }
        });
    }

    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);

        return _sessionPool.DisposeAsync();
    }
}

public class QueryTx
{
    private readonly Session _session;
    private readonly TxMode _txMode;

    private string? TxId { get; set; }
    private bool Commited { get; set; }

    private TransactionControl TxControl(bool autocommit) => TxId == null
        ? new TransactionControl { BeginTx = _txMode.TransactionSettings(), CommitTx = autocommit }
        : new TransactionControl { TxId = TxId, CommitTx = autocommit };

    internal QueryTx(Session session, TxMode txMode)
    {
        _session = session;
        _txMode = txMode;
    }

    public async Task<IReadOnlyList<Value.ResultSet.Row>> ReadAllRows(string query,
        Dictionary<string, YdbValue>? parameters = null, bool commit = false, ExecuteQuerySettings? settings = null)
    {
        List<Value.ResultSet.Row> rows = new();

        await foreach (var part in _session.ExecuteQuery(query, parameters, settings, TxControl(commit)))
        {
            if (part.Status.IsNotSuccess)
            {
                throw new UnexpectedResultException(part.Status);
            }

            TxId ??= part.TxId;

            rows.AddRange(part.ResultSet?.FromProto().Rows!);
        }

        Commited = commit | Commited;

        return rows.AsReadOnly();
    }

    public async Task Exec(string query, Dictionary<string, YdbValue>? parameters = null,
        ExecuteQuerySettings? settings = null, bool commit = false)
    {
        await foreach (var part in _session.ExecuteQuery(query, parameters, settings, TxControl(commit)))
        {
            if (part.Status.IsNotSuccess)
            {
                throw new UnexpectedResultException(part.Status);
            }
        }

        Commited = commit | Commited;
    }

    public Task Rollback()
    {
        if (TxId == null)
        {
            throw new UnexpectedResultException("Transaction isn't started!",
                new Status(StatusCode.PreconditionFailed));
        }

        Commited = true;

        return _session.RollbackTransaction(TxId!);
    }

    internal async Task<Status> Commit()
    {
        var status = Commited ? Status.Success : await _session.CommitTransaction(TxId!);

        return status;
    }
}
