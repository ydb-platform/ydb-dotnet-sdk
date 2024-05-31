using System.Collections.Immutable;
using Microsoft.Extensions.Logging;
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

        var rpc = new QueryServiceRpc(driver);

        _sessionPool = new SessionPool(rpc, driver.LoggerFactory.CreateLogger<SessionPool>(), config.SizeSessionPool);
    }

    // method for ADO.NET
    internal Task<(Status, Session?)> GetSession()
    {
        return _sessionPool.GetSession();
    }

    // TODO Retry policy and may be move to SessionPool method
    public async Task<Result<T>> ExecOnSession<T>(Func<Session, Task<Result<T>>> onSession) where T : class
    {
        Session? session = null;
        try
        {
            var (status, newSession) = await _sessionPool.GetSession();

            if (status.IsSuccess)
            {
                session = newSession;

                return await onSession(session!);
            }

            status.EnsureSuccess(); // throw exception; - fixed
        }
        catch (Driver.TransportException e)
        {
            // _logger.LogError(); - todo
        }
        finally
        {
            session?.Release();
        }

        throw new Exception();
    }

    // Reading the result set stream into memory
    public Task<Result<IReadOnlyList<Value.ResultSet.Row>>> ReadAllRows(string query,
        Dictionary<string, YdbValue>? parameters = null, TxMode txMode = TxMode.None,
        ExecuteQuerySettings? settings = null)
    {
        return ExecOnSession<IReadOnlyList<Value.ResultSet.Row>>(async session =>
        {
            List<Value.ResultSet.Row> rows = new();

            await foreach (var (status, resultSet) in session.ExecuteQuery(query, parameters, txMode, settings))
            {
                if (status.IsNotSuccess)
                {
                    return Result.Fail<IReadOnlyList<Value.ResultSet.Row>>(status);
                }

                rows.AddRange(resultSet?.FromProto().Rows ?? ImmutableList<Value.ResultSet.Row>.Empty);
            }

            return Result.Success<IReadOnlyList<Value.ResultSet.Row>>(rows.AsReadOnly());
        });
    }

    public async Task<(Status, Value.ResultSet.Row?)> ReadRow(string query,
        Dictionary<string, YdbValue>? parameters = null, TxMode txMode = TxMode.None,
        ExecuteQuerySettings? settings = null)
    {
        var result = await ReadAllRows(query, parameters, txMode, settings);

        return result is { IsSuccess: true, Value.Count: > 0 }
            ? (result.Status, result.Value[0])
            : (result.Status, null);
    }

    public async Task<Status> Exec(string query, Dictionary<string, YdbValue>? parameters = null,
        TxMode txMode = TxMode.None, ExecuteQuerySettings? settings = null)
    {
        var result = await ReadAllRows(query, parameters, txMode, settings);

        return result.Status;
    }

    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);

        return _sessionPool.DisposeAsync();
    }
}
