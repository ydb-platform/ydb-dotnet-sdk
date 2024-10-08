using System.Data;
using System.Data.Common;
using Ydb.Sdk.Services.Query;
using static System.Data.IsolationLevel;

namespace Ydb.Sdk.Ado;

public sealed class YdbConnection : DbConnection
{
    private static readonly YdbConnectionStringBuilder DefaultSettings = new();

    private bool _disposed;

    private YdbConnectionStringBuilder ConnectionStringBuilder { get; set; }

    internal Session Session
    {
        get
        {
            EnsureConnectionOpen();

            return _session;
        }
        private set => _session = value;
    }

    private Session _session = null!;

    public YdbConnection()
    {
        ConnectionStringBuilder = DefaultSettings;
    }

    public YdbConnection(string connectionString)
    {
        ConnectionStringBuilder = new YdbConnectionStringBuilder(connectionString);
    }

    public YdbConnection(YdbConnectionStringBuilder connectionStringBuilder)
    {
        ConnectionStringBuilder = connectionStringBuilder;
    }

    protected override YdbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        EnsureConnectionOpen();

        return BeginTransaction(isolationLevel switch
        {
            Serializable or Unspecified => TxMode.SerializableRw,
            _ => throw new ArgumentException("Unsupported isolationLevel: " + isolationLevel)
        });
    }

    public new YdbTransaction BeginTransaction(IsolationLevel isolationLevel)
    {
        return BeginDbTransaction(isolationLevel);
    }

    public YdbTransaction BeginTransaction(TxMode txMode = TxMode.SerializableRw)
    {
        EnsureConnectionOpen();

        return new YdbTransaction(this, txMode);
    }

    public override void ChangeDatabase(string databaseName)
    {
    }

    public override void Close()
    {
        CloseAsync().GetAwaiter().GetResult();
    }

    public override void Open()
    {
        OpenAsync().GetAwaiter().GetResult();
    }

    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        EnsureConnectionClosed();

        try
        {
            Session = await PoolManager.GetSession(ConnectionStringBuilder);
        }
        catch (Exception e)
        {
            throw e switch
            {
                Driver.TransportException transportException => new YdbException(transportException.Status),
                StatusUnsuccessfulException unsuccessfulException => new YdbException(unsuccessfulException.Status),
                _ => new YdbException("Cannot get session", e)
            };
        }

        ConnectionState = ConnectionState.Open;
    }

    public override async Task CloseAsync()
    {
        if (State == ConnectionState.Closed)
        {
            return;
        }

        try
        {
            if (LastReader is { IsClosed: false })
            {
                await LastReader.CloseAsync();
            }

            if (LastTransaction is { Completed: false })
            {
                await LastTransaction.RollbackAsync();
            }

            ConnectionState = ConnectionState.Closed;
        }
        finally
        {
            await _session.Release();
        }
    }

    public override string ConnectionString
    {
        get => ConnectionStringBuilder.ConnectionString;
#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
        set
#pragma warning restore CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
        {
            EnsureConnectionClosed();

            // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
            ConnectionStringBuilder = value != null ? new YdbConnectionStringBuilder(value) : DefaultSettings;
        }
    }

    public override string Database => ConnectionStringBuilder.Database;

    public override ConnectionState State => ConnectionState;

    private ConnectionState ConnectionState { get; set; } = ConnectionState.Closed; // Invoke AsyncOpen()

    internal YdbDataReader? LastReader { get; set; }
    internal string LastCommand { get; set; } = string.Empty;
    internal YdbTransaction? LastTransaction { get; set; }
    internal bool IsBusy => LastReader is { IsClosed: false };

    public override string DataSource => string.Empty; // TODO
    public override string ServerVersion => string.Empty; // TODO

    protected override YdbCommand CreateDbCommand()
    {
        return new YdbCommand(this);
    }

    public new YdbCommand CreateCommand()
    {
        return CreateDbCommand();
    }

    internal void EnsureConnectionOpen()
    {
        if (ConnectionState == ConnectionState.Closed)
        {
            throw new InvalidOperationException("Connection is closed");
        }
    }

    private void EnsureConnectionClosed()
    {
        if (ConnectionState != ConnectionState.Closed)
        {
            throw new InvalidOperationException("Connection already open");
        }
    }

    /// <summary>
    /// Releases all resources used by the <see cref="YdbConnection"/>.
    /// </summary>
    /// <param name="disposing"><see langword="true"/> when called from <see cref="Dispose"/>;
    /// <see langword="false"/> when being called from the finalizer.</param>
    protected override void Dispose(bool disposing)
    {
        if (_disposed)
            return;
        if (disposing)
            Close();
        _disposed = true;
    }

    /// <summary>
    /// Releases all resources used by the <see cref="YdbConnection"/>.
    /// </summary>
    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        await CloseAsync();
        _disposed = true;
    }

    /// <summary>
    /// Clears the connection pool. All idle physical connections in the pool of the given connection are
    /// immediately closed, and any busy connections which were opened before <see cref="ClearPool"/> was called
    /// will be closed when returned to the pool.
    /// </summary>
    public static Task ClearPool(YdbConnection connection)
    {
        return PoolManager.ClearPool(connection.ConnectionString);
    }

    /// <summary>
    /// Clear all connection pools. All idle physical connections in all pools are immediately closed, and any busy
    /// connections which were opened before <see cref="ClearAllPools"/> was called will be closed when returned
    /// to their pool.
    /// </summary>
    public static Task ClearAllPools()
    {
        return PoolManager.ClearAllPools();
    }
}
