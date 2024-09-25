using System.Data;
using System.Data.Common;
using Ydb.Query;
using Ydb.Sdk.Services.Query;

namespace Ydb.Sdk.Ado;

public sealed class YdbTransaction : DbTransaction
{
    private readonly TxMode _txMode;

    internal string? TxId { get; set; }
    internal bool Completed { get; set; }
    internal bool Failed { get; set; }

    internal TransactionControl? TransactionControl => Completed || Failed
        ? null
        : TxId == null
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
        await FinishTransaction(txId => DbConnection.Session.CommitTransaction(txId));
    }

    public override void Rollback()
    {
        RollbackAsync().GetAwaiter().GetResult();
    }

    // TODO propagate cancellation token
    public override async Task RollbackAsync(CancellationToken cancellationToken = new())
    {
        if (Failed)
        {
            Failed = false;
            Completed = true; // make completed

            return;
        }

        await FinishTransaction(txId => DbConnection.Session.RollbackTransaction(txId));
    }

    protected override YdbConnection DbConnection { get; }

    public override IsolationLevel IsolationLevel => _txMode == TxMode.SerializableRw
        ? IsolationLevel.Serializable
        : IsolationLevel.Unspecified;

    private async Task FinishTransaction(Func<string, Task<Status>> finishMethod)
    {
        if (Failed || Completed || DbConnection.State == ConnectionState.Closed)
        {
            throw new InvalidOperationException("This YdbTransaction has completed; it is no longer usable");
        }

        if (DbConnection.IsBusy)
        {
            throw new YdbOperationInProgressException(DbConnection);
        }

        Completed = true;

        if (TxId == null)
        {
            return; // transaction isn't started
        }

        try
        {
            var status = await finishMethod(TxId); // Commit or Rollback

            if (status.IsNotSuccess)
            {
                throw new YdbException(status);
            }
        }
        catch (Driver.TransportException e)
        {
            throw new YdbException(e.Status);
        }
    }
}
