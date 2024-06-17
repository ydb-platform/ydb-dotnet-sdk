using System.Data;
using System.Data.Common;

namespace Ydb.Sdk.Ado;

public sealed class YdbTransaction : DbTransaction
{
    internal YdbTransaction(YdbConnection ydbConnection, IsolationLevel isolationLevel)
    {
        DbConnection = ydbConnection;
        IsolationLevel = isolationLevel;
    }

    public override void Commit()
    {
        CommitAsync().GetAwaiter().GetResult();
    }

    // TODO propagate cancellation token
    public override async Task CommitAsync(CancellationToken cancellationToken = new())
    {
        await DbConnection.YdbConnectionState.Commit();

        DbConnection.NextOutTransactionState();
    }

    public override void Rollback()
    {
        RollbackAsync().GetAwaiter().GetResult();
    }

    // TODO propagate cancellation token
    public override async Task RollbackAsync(CancellationToken cancellationToken = new())
    {
        await DbConnection.YdbConnectionState.Rollback();

        DbConnection.NextOutTransactionState();
    }

    protected override YdbConnection DbConnection { get; }

    public override IsolationLevel IsolationLevel { get; }
}
