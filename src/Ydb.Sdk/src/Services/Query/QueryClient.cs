using System.Collections.Immutable;
using Ydb.Sdk.Pool;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Services.Query;

public class QueryClientConfig
{
    public int MaxSessionPool
    {
        get => _masSessionPool;
        init
        {
            if (value <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "Invalid max session pool: " + value);
            }

            _masSessionPool = value;
        }
    }

    private readonly int _masSessionPool = SessionPoolDefaultSettings.MaxSessionPool;

    public int CreateSessionTimeout
    {
        get => _createSessionTimeout;
        init
        {
            if (value < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value,
                    "Invalid create session timeout: " + value);
            }

            _createSessionTimeout = value;
        }
    }

    private readonly int _createSessionTimeout = SessionPoolDefaultSettings.CreateSessionTimeoutSeconds;
}

public class QueryClient : IAsyncDisposable
{
    private readonly SessionPool _sessionPool;

    public QueryClient(IDriver driver, QueryClientConfig? config = null)
    {
        config ??= new QueryClientConfig();

        _sessionPool = new SessionPool(driver,
            new SessionPoolConfig(
                CreateSessionTimeout: config.CreateSessionTimeout,
                MaxSessionPool: config.MaxSessionPool
            )
        );
    }

    public Task<T> Stream<T>(string query, Func<ExecuteQueryStream, Task<T>> onStream,
        Dictionary<string, YdbValue>? parameters = null, TxMode txMode = TxMode.NoTx,
        ExecuteQuerySettings? settings = null) =>
        _sessionPool.ExecOnSession(async session => await onStream(new ExecuteQueryStream(
            await session.ExecuteQuery(query, parameters, settings, txMode.TransactionControl())))
        );

    public Task Stream(string query, Func<ExecuteQueryStream, Task> onStream,
        Dictionary<string, YdbValue>? parameters = null, TxMode txMode = TxMode.NoTx,
        ExecuteQuerySettings? settings = null) =>
        Stream<object>(query, async stream =>
        {
            await onStream(stream);
            return None;
        }, parameters, txMode, settings);

    public Task<IReadOnlyList<Value.ResultSet.Row>> ReadAllRows(string query,
        Dictionary<string, YdbValue>? parameters = null, TxMode txMode = TxMode.NoTx,
        ExecuteQuerySettings? settings = null) =>
        Stream<IReadOnlyList<Value.ResultSet.Row>>(query, async stream =>
        {
            await using var uStream = stream;
            List<Value.ResultSet.Row> rows = new();

            await foreach (var part in uStream)
            {
                if (part.ResultSetIndex > 0)
                {
                    break;
                }

                rows.AddRange(part.ResultSet?.Rows ?? ImmutableList<Value.ResultSet.Row>.Empty);
            }

            return rows.AsReadOnly();
        }, parameters, txMode, settings);

    public async Task<Value.ResultSet.Row?> ReadRow(string query,
        Dictionary<string, YdbValue>? parameters = null, TxMode txMode = TxMode.NoTx,
        ExecuteQuerySettings? settings = null)
    {
        var result = await ReadAllRows(query, parameters, txMode, settings);

        return result is { Count: > 0 } ? result[0] : null;
    }

    public async Task Exec(string query, Dictionary<string, YdbValue>? parameters = null,
        TxMode txMode = TxMode.NoTx, ExecuteQuerySettings? settings = null) =>
        await Stream(query, async stream =>
        {
            await using var uStream = stream;

            _ = await stream.MoveNextAsync();
        }, parameters, txMode, settings);

    public Task<T> DoTx<T>(Func<QueryTx, Task<T>> queryTx, TxMode txMode = TxMode.SerializableRw)
    {
        if (txMode == TxMode.NoTx)
        {
            throw new ArgumentException("DoTx requires a txMode other than None");
        }

        return _sessionPool.ExecOnSession<T>(async session =>
        {
            var tx = new QueryTx(session, txMode);

            try
            {
                var result = await queryTx(tx);

                await tx.Commit();

                return result;
            }
            catch (StatusUnsuccessfulException)
            {
                throw;
            }
            catch (Driver.TransportException)
            {
                throw;
            }
            catch (Exception)
            {
                await tx.Rollback();
                throw;
            }
            finally
            {
                await session.Release();
            }
        });
    }

    private static readonly object None = new();

    public async Task DoTx(Func<QueryTx, Task> queryTx, TxMode txMode = TxMode.SerializableRw) =>
        await DoTx<object>(async tx =>
        {
            await queryTx(tx);

            return None;
        }, txMode);

    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);

        return _sessionPool.DisposeAsync();
    }
}
