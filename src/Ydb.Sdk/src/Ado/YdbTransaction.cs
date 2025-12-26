using System.Data;
using System.Data.Common;
using Ydb.Query;
using Ydb.Sdk.Ado.Transaction;
using Ydb.Sdk.Ado.TxWriter;

namespace Ydb.Sdk.Ado;

/// <summary>
/// Represents a YDB transaction. This class cannot be inherited.
/// </summary>
/// <remarks>
/// YdbTransaction represents a database transaction in YDB. It provides methods to commit or rollback
/// changes made within the transaction. The transaction mode determines the isolation level and
/// consistency guarantees.
/// 
/// <para>
/// For more information about YDB transaction modes, see:
/// <see href="https://ydb.tech/docs/en/concepts/transactions">YDB Transactions Documentation</see>.
/// </para>
/// </remarks>
public sealed class YdbTransaction : DbTransaction
{
    private readonly TransactionMode _transactionMode;

    private bool _failed;
    private YdbConnection? _ydbConnection;
    private bool _isDisposed;

    /// <summary>
    /// Gets or sets the transaction identifier.
    /// </summary>
    /// <remarks>
    /// The transaction ID is assigned by YDB when the transaction is started.
    /// This property is used internally for transaction management.
    /// </remarks>
    internal string? TxId { get; set; }

    /// <summary>
    /// Gets a value indicating whether the transaction has been completed.
    /// </summary>
    /// <remarks>
    /// A transaction is considered completed when it has been committed, rolled back, or failed.
    /// Once completed, the transaction cannot be used for further operations.
    /// </remarks>
    internal bool Completed { get; private set; }

    /// <summary>
    /// Gets or sets a value indicating whether the transaction has failed.
    /// </summary>
    /// <remarks>
    /// When true, indicates that the transaction has been rolled back by the server.
    /// A failed transaction cannot be committed. The <see cref="Rollback"/> method
    /// can be called to mark the transaction as completed.
    /// </remarks>
    internal bool Failed
    {
        private get => _failed;
        set
        {
            _failed = value;
            Completed = true;
        }
    }

    /// <summary>
    /// Gets the transaction control for YDB operations.
    /// </summary>
    /// <remarks>
    /// Returns null if the transaction is completed, otherwise returns the appropriate
    /// transaction control based on whether the transaction has been started.
    /// </remarks>
    internal TransactionControl? TransactionControl => Completed
        ? null
        : TxId == null
            ? new TransactionControl { BeginTx = _transactionMode.TransactionSettings() }
            : new TransactionControl { TxId = TxId };

    internal YdbTransaction(YdbConnection ydbConnection, TransactionMode transactionMode)
    {
        _ydbConnection = ydbConnection;
        _transactionMode = transactionMode;
    }

    /// <summary>
    /// Commits the database transaction.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the transaction has already been completed or the connection is closed.
    /// </exception>
    /// <exception cref="YdbOperationInProgressException">
    /// Thrown when another operation is in progress on the connection.
    /// </exception>
    /// <exception cref="YdbException">
    /// Thrown when the commit operation fails.
    /// </exception>
    public override void Commit() => CommitAsync().GetAwaiter().GetResult();

    /// <summary>
    /// Asynchronously commits the database transaction.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the transaction has already been completed or the connection is closed.
    /// </exception>
    /// <exception cref="YdbOperationInProgressException">
    /// Thrown when another operation is in progress on the connection.
    /// </exception>
    /// <exception cref="YdbException">
    /// Thrown when the commit operation fails.
    /// </exception>
    public override async Task CommitAsync(CancellationToken cancellationToken = new())
    {
        // Flush all TxWriters before committing
        await FlushAllTxWritersAsync(cancellationToken);
        
        await FinishTransaction(txId => DbConnection!.Session.CommitTransaction(txId, cancellationToken));
    }

    /// <summary>
    /// Rolls back the database transaction.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the transaction has already been completed or the connection is closed.
    /// </exception>
    /// <exception cref="YdbOperationInProgressException">
    /// Thrown when another operation is in progress on the connection.
    /// </exception>
    /// <exception cref="YdbException">
    /// Thrown when the rollback operation fails.
    /// </exception>
    public override void Rollback() => RollbackAsync().GetAwaiter().GetResult();

    /// <summary>
    /// Asynchronously rolls back the database transaction.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the transaction has already been completed or the connection is closed.
    /// </exception>
    /// <exception cref="YdbOperationInProgressException">
    /// Thrown when another operation is in progress on the connection.
    /// </exception>
    /// <exception cref="YdbException">
    /// Thrown when the rollback operation fails.
    /// </exception>
    public override async Task RollbackAsync(CancellationToken cancellationToken = new())
    {
        if (Failed)
        {
            Failed = false;

            return;
        }

        await FinishTransaction(txId => DbConnection!.Session.RollbackTransaction(txId, cancellationToken));
    }

    /// <summary>
    /// Gets the database connection associated with this transaction.
    /// </summary>
    /// <returns>The YdbConnection associated with this transaction, or null if disposed.</returns>
    /// <exception cref="ObjectDisposedException">
    /// Thrown when the transaction has been disposed.
    /// </exception>
    protected override YdbConnection? DbConnection
    {
        get
        {
            CheckDisposed();
            return _ydbConnection;
        }
    }

    /// <summary>
    /// Gets the isolation level of this transaction.
    /// </summary>
    /// <remarks>
    /// Returns Serializable for SerializableRw transaction mode, otherwise Unspecified.
    /// The actual isolation level depends on the transaction mode used when creating the transaction.
    /// </remarks>
    public override IsolationLevel IsolationLevel => _transactionMode == TransactionMode.SerializableRw
        ? IsolationLevel.Serializable
        : IsolationLevel.Unspecified;

    private async Task FlushAllTxWritersAsync(CancellationToken cancellationToken)
    {
        if (DbConnection == null)
        {
            return;
        }

        if (DbConnection.TxWriters.Count == 0)
        {
            return;
        }

        var flushTasks = new List<Task>();
        foreach (var writer in DbConnection.TxWriters)
        {
            if (writer is IInternalTxWriter internalWriter)
            {
                flushTasks.Add(internalWriter.FlushAsync(cancellationToken));
            }
        }

        try
        {
            await Task.WhenAll(flushTasks);
        }
        catch (Exception ex)
        {
            // If flush fails, dispose all writers to clean up
            DisposeAllTxWriters();
            throw new TxTopicWriterException("Failed to flush transactional topic writers before commit", ex);
        }
    }

    private void DisposeAllTxWriters()
    {
        if (DbConnection == null)
        {
            return;
        }

        foreach (var writer in DbConnection.TxWriters)
        {
            if (writer is IDisposableTxWriter disposableWriter)
            {
                disposableWriter.Dispose();
            }
        }

        DbConnection.TxWriters.Clear();
    }

    private async Task FinishTransaction(Func<string, Task> finishMethod)
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
                // Dispose writers even if transaction wasn't started
                DisposeAllTxWriters();
                return; // transaction isn't started
            }

            await finishMethod(TxId); // Commit or Rollback
            
            // Dispose writers after successful commit or rollback
            DisposeAllTxWriters();
        }
        catch (YdbException e)
        {
            Failed = true;

            DbConnection.OnNotSuccessStatusCode(e.Code);
            
            // Dispose writers on error
            DisposeAllTxWriters();

            throw;
        }
        finally
        {
            _ydbConnection = null;
        }
    }

    /// <summary>
    /// Releases the unmanaged resources used by the YdbTransaction and optionally releases the managed resources.
    /// </summary>
    /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
    /// <remarks>
    /// If the transaction is not completed, it will be rolled back before disposal.
    /// </remarks>
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

    /// <summary>
    /// Asynchronously releases the unmanaged resources used by the YdbTransaction.
    /// </summary>
    /// <returns>A ValueTask representing the asynchronous disposal operation.</returns>
    /// <remarks>
    /// If the transaction is not completed, it will be rolled back before disposal.
    /// </remarks>
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
