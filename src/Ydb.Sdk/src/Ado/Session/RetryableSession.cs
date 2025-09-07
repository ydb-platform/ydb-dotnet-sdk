using Ydb.Query;
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

    private static YdbException NotSupportedTransaction => new("Transactions are not supported in retryable sessions");
}

internal sealed class InMemoryServerStream : IServerStream<ExecuteQueryResponsePart>
{
    private readonly ISessionSource _sessionSource;
    private readonly YdbRetryPolicyExecutor _ydbRetryPolicyExecutor;
    private readonly string _query;
    private readonly Dictionary<string, TypedValue> _parameters;
    private readonly GrpcRequestSettings _settings;

    private List<ExecuteQueryResponsePart>? _responses;
    private int _iterator;

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
        if (_responses is not null)
        {
            return ++_iterator < _responses.Count;
        }

        _responses = new List<ExecuteQueryResponsePart>();

        return await _ydbRetryPolicyExecutor.ExecuteAsync(async ct =>
        {
            using var session = await _sessionSource.OpenSession(ct);

            var serverStream = await session.ExecuteQuery(_query, _parameters, _settings, null);
            while (await serverStream.MoveNextAsync(ct))
            {
                _responses.Add(serverStream.Current);
            }

            return _responses.Count > 0;
        }, cancellationToken);
    }

    public ExecuteQueryResponsePart Current => _responses is not null && _iterator < _responses.Count
        ? _responses[_iterator]
        : throw new InvalidOperationException("No response found");

    public void Dispose()
    {
    }
}
