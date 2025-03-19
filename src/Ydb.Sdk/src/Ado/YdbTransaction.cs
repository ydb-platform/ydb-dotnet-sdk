using System.Data;
using System.Data.Common;
using Ydb.Query;
using Ydb.Sdk.Services.Query;

namespace Ydb.Sdk.Ado;

public sealed class YdbTransaction : DbTransaction
{
    private readonly TxMode _txMode;

    private bool _failed;
    private YdbConnection? _ydbConnection;
    private bool _isDisposed;

    internal string? TxId { get; set; }
    internal bool Completed { get; private set; }

    internal bool Failed
    {
        private get => _failed;
        set
        {
            _failed = value;
            Completed = true;
        }
    }

    internal TransactionControl? TransactionControl => Completed
        ? null
        : TxId == null
            ? new TransactionControl { BeginTx = _txMode.TransactionSettings() }
            : new TransactionControl { TxId = TxId };

    internal YdbTransaction(YdbConnection ydbConnection, TxMode txMode)
    {
        _ydbConnection = ydbConnection;
        _txMode = txMode;
    }

    public override void Commit() => CommitAsync().GetAwaiter().GetResult();

    // TODO propagate cancellation token
    public override async Task CommitAsync(CancellationToken cancellationToken = new()) =>
        await FinishTransaction(txId => DbConnection!.Session.CommitTransaction(txId));

    public override void Rollback() => RollbackAsync().GetAwaiter().GetResult();

    // TODO propagate cancellation token
    public override async Task RollbackAsync(CancellationToken cancellationToken = new())
    {
        if (Failed)
        {
            Failed = false;

            return;
        }

        await FinishTransaction(txId => DbConnection!.Session.RollbackTransaction(txId));
    }

    protected override YdbConnection? DbConnection
    {
        get
        {
            CheckDisposed();
            return _ydbConnection;
        }
    }

    public override IsolationLevel IsolationLevel => _txMode == TxMode.SerializableRw
        ? IsolationLevel.Serializable
        : IsolationLevel.Unspecified;

    private async Task FinishTransaction(Func<string, Task<Status>> finishMethod)
    {
        if (DbConnection?.State == ConnectionState.Closed || Completed)
        {
            throw new InvalidOperationException("This YdbTransaction has completed; it is no longer usable");
        }

        if (DbConnection!.IsBusy)
        {
            throw new YdbOperationInProgressException(DbConnection);
        }

        try
        {
            Completed = true;

            if (TxId == null)
            {
                return; // transaction isn't started
            }

            var status = await finishMethod(TxId); // Commit or Rollback

            if (status.IsNotSuccess)
            {
                Failed = true;

                DbConnection.Session.OnStatus(status);

                throw new YdbException(status);
            }
        }
        catch (Driver.TransportException e)
        {
            Failed = true;

            DbConnection.Session.OnStatus(e.Status);

            throw new YdbException(e.Status);
        }
        finally
        {
            _ydbConnection = null;
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (_isDisposed || !disposing)
            return;

        if (!Completed)
        {
            Rollback();
        }

        _isDisposed = true;
    }

    public override async ValueTask DisposeAsync()
    {
        if (_isDisposed)
            return;

        if (!Completed)
        {
            await RollbackAsync();
        }

        _isDisposed = true;
    }

    private void CheckDisposed()
    {
        if (_isDisposed)
        {
            throw new ObjectDisposedException(nameof(YdbTransaction));
        }
    }
}
