using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Value;
using Session = Ydb.Sdk.Services.Query.Pool.Session;

namespace Ydb.Sdk.Ado;

internal interface IYdbConnectionState
{
    Task Commit();

    Task Rollback();

    IReadOnlyList<ExecuteQueryPart> ExecuteQuery(string query, Dictionary<string, YdbValue>? parameters);

    bool IsClosed => false;
}

internal sealed class OutTransactionState : IYdbConnectionState
{
    public Task Commit()
    {
        return Task.CompletedTask;
    }

    public Task Rollback()
    {
        return Task.CompletedTask;
    }

    public IReadOnlyList<ExecuteQueryPart> ExecuteQuery(string query, Dictionary<string, YdbValue>? parameters)
    {
        throw new NotImplementedException(); // TODO QueryClient
    }
}

internal sealed class InTransactionState : IYdbConnectionState
{
    private readonly Session _session;

    internal string? TxId { get; set; }

    internal InTransactionState(Session session)
    {
        _session = session;
    }

    public async Task Commit()
    {
        await TransactionFinish(txId => _session.RollbackTransaction(txId));
    }

    public async Task Rollback()
    {
        await TransactionFinish(txId => _session.RollbackTransaction(txId));
    }

    private async Task TransactionFinish(Func<string, Task<Status>> finishMethod)
    {
        if (TxId == null)
        {
            return;
        }

        try
        {
            var status = await finishMethod(TxId); // TODO Maybe retry on transport status

            if (status.IsNotSuccess)
            {
                throw new YdbAdoException(status);
            }
        }
        finally
        {
            _session.Release();
        }
    }

    public IReadOnlyList<ExecuteQueryPart> ExecuteQuery(string query, Dictionary<string, YdbValue>? parameters)
    {
        throw new NotImplementedException();
    }
}

internal sealed class ClosedState : IYdbConnectionState
{
    private const string ClosedStateMessage = "Connection closed";

    public Task Commit()
    {
        throw new YdbAdoException(ClosedStateMessage);
    }

    public Task Rollback()
    {
        throw new YdbAdoException(ClosedStateMessage);
    }

    public IReadOnlyList<ExecuteQueryPart> ExecuteQuery(string query, Dictionary<string, YdbValue>? parameters)
    {
        throw new YdbAdoException(ClosedStateMessage);
    }

    public bool IsClosed => true;
}
