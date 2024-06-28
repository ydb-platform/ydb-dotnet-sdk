using System.Data;
using System.Data.Common;
using Ydb.Query;
using Ydb.Sdk.Services.Query;

namespace Ydb.Sdk.Ado;

public sealed class YdbTransaction : DbTransaction
{
    private readonly TxMode _txMode;

    internal string? TxId { get; set; }

    internal TransactionControl TransactionControl => TxId == null
        ? new TransactionControl { BeginTx = _txMode.TransactionSettings() }
        : new TransactionControl { TxId = TxId };

    internal YdbTransaction(YdbConnection ydbConnection, TxMode txMode)
    {
        DbConnection = ydbConnection;
        _txMode = txMode;
    }

    public override void Commit()
    {
        CommitAsync().GetAwaiter().GetResult();
    }

    // TODO propagate cancellation token
    public override async Task CommitAsync(CancellationToken cancellationToken = new())
    {
        if (TxId == null)
        {
            return;
        }

        var status = await DbConnection.Session.CommitTransaction(TxId);

        status.EnsureSuccess();
    }

    public override void Rollback()
    {
        RollbackAsync().GetAwaiter().GetResult();
    }

    // TODO propagate cancellation token
    public override async Task RollbackAsync(CancellationToken cancellationToken = new())
    {
        if (TxId == null)
        {
            return;
        }

        var status = await DbConnection.Session.RollbackTransaction(TxId);

        status.EnsureSuccess();
    }

    protected override YdbConnection DbConnection { get; }

    public override IsolationLevel IsolationLevel => _txMode == TxMode.SerializableRw
        ? IsolationLevel.Serializable
        : IsolationLevel.Unspecified;
}
