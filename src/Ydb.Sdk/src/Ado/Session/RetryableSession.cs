using Ydb.Query;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Ado.RetryPolicy;

namespace Ydb.Sdk.Ado.Session;

internal class RetryableSession : ISession
{
    private readonly ISessionSource _sessionSource;
    private readonly YdbRetryPolicyExecutor _retryPolicyExecutor;

    internal RetryableSession(ISessionSource sessionSource, YdbRetryPolicyExecutor retryPolicyExecutor)
    {
        _sessionSource = sessionSource;
        _retryPolicyExecutor = retryPolicyExecutor;
    }

    public IDriver Driver => throw new NotImplementedException();
    public bool IsBroken => false;

    public ValueTask<IServerStream<ExecuteQueryResponsePart>> ExecuteQuery(
        string query,
        Dictionary<string, TypedValue> parameters,
        GrpcRequestSettings settings,
        TransactionControl? txControl
    )
    {
        if (txControl is not null && !txControl.CommitTx)
        {
            throw NotSupportedTransaction;
        }

        return new ValueTask<IServerStream<ExecuteQueryResponsePart>>(
            new InMemoryServerStream(_sessionSource, _retryPolicyExecutor, query, parameters, settings));
    }

    public Task CommitTransaction(string txId, CancellationToken cancellationToken = default) =>
        throw NotSupportedTransaction;

    public Task RollbackTransaction(string txId, CancellationToken cancellationToken = default) =>
        throw NotSupportedTransaction;

    public void OnNotSuccessStatusCode(StatusCode code)
    {
    }

    public void Dispose()
    {
    }

    private static YdbException NotSupportedTransaction => new("Transactions are not supported in retryable session");
}

internal sealed class InMemoryServerStream : IServerStream<ExecuteQueryResponsePart>
{
    private readonly ISessionSource _sessionSource;
    private readonly YdbRetryPolicyExecutor _ydbRetryPolicyExecutor;
    private readonly string _query;
    private readonly Dictionary<string, TypedValue> _parameters;
    private readonly GrpcRequestSettings _settings;

    private List<ExecuteQueryResponsePart>? _responses;
    private int _index = -1;

    public InMemoryServerStream(
        ISessionSource sessionSource,
        YdbRetryPolicyExecutor retryPolicyExecutor,
        string query,
        Dictionary<string, TypedValue> parameters,
        GrpcRequestSettings settings)
    {
        _sessionSource = sessionSource;
        _ydbRetryPolicyExecutor = retryPolicyExecutor;
        _query = query;
        _parameters = parameters;
        _settings = settings;
    }

    public async Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        _responses ??= await _ydbRetryPolicyExecutor.ExecuteAsync<List<ExecuteQueryResponsePart>>(async ct =>
        {
            using var session = await _sessionSource.OpenSession(ct);

            try
            {
                var responses = new List<ExecuteQueryResponsePart>();
                var serverStream = await session.ExecuteQuery(_query, _parameters, _settings, null);

                while (await serverStream.MoveNextAsync(ct))
                {
                    var current = serverStream.Current;

                    if (current.Status.IsNotSuccess())
                    {
                        throw YdbException.FromServer(current.Status, current.Issues);
                    }

                    responses.Add(serverStream.Current);
                }

                return responses;
            }
            catch (YdbException e)
            {
                session.OnNotSuccessStatusCode(e.Code);
                throw;
            }
        }, cancellationToken);

        return ++_index < _responses.Count;
    }

    public ExecuteQueryResponsePart Current => _responses is not null && _index >= 0 && _index < _responses.Count
        ? _responses[_index]
        : throw new InvalidOperationException("Enumeration has not started or has already finished");

    public void Dispose()
    {
    }
}
