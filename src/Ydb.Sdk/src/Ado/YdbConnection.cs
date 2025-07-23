using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Ydb.Operations;
using Ydb.Sdk.Ado.BulkUpsert;
using Ydb.Sdk.Ado.Session;
using Ydb.Sdk.Services.Query;
using Ydb.Table;
using static System.Data.IsolationLevel;

namespace Ydb.Sdk.Ado;

/// <summary>
/// YDB database connection implementation.
/// </summary>
public sealed class YdbConnection : DbConnection
{
    private static readonly StateChangeEventArgs ClosedToOpenEventArgs =
        new(ConnectionState.Closed, ConnectionState.Open);

    private static readonly StateChangeEventArgs OpenToClosedEventArgs =
        new(ConnectionState.Open, ConnectionState.Closed);

    private bool _disposed;
    private YdbConnectionStringBuilder? _connectionStringBuilder;

    private YdbConnectionStringBuilder ConnectionStringBuilder
    {
        get => _connectionStringBuilder ??
               throw new InvalidOperationException("The ConnectionString property has not been initialized.");
        [param: AllowNull] init => _connectionStringBuilder = value;
    }

    // ISession для абстракции!
    internal ISession Session
    {
        get
        {
            ThrowIfConnectionClosed();
            return _session;
        }
        private set => _session = value;
    }

    private ISession _session = null!;

    public YdbConnection() { }

    public YdbConnection(string connectionString)
    {
        ConnectionStringBuilder = new YdbConnectionStringBuilder(connectionString);
    }

    public YdbConnection(YdbConnectionStringBuilder connectionStringBuilder)
    {
        ConnectionStringBuilder = connectionStringBuilder;
    }

    public YdbBulkUpsertImporter<T> BeginBulkUpsert<T>(
        string tablePath,
        BulkUpsertOptions? options = null,
        int maxBatchSizeBytes = 64 * 1024 * 1024) // 64 Mb
    {
        return new YdbBulkUpsertImporter<T>(this, tablePath, options, maxBatchSizeBytes);
    }

    internal async Task BulkUpsertInternalAsync<T>(
        string tablePath,
        IReadOnlyCollection<T> rows,
        BulkUpsertOptions options,
        CancellationToken cancellationToken)
    {
        if (CurrentTransaction is { Completed: false })
            throw new InvalidOperationException("BulkUpsert does not support working within a transaction");
        var req = new BulkUpsertRequest
        {
            Table = tablePath,
            OperationParams = new OperationParams(),
            Rows = TypedValueFactory.FromObjects(rows)
        };
        
        var sessionImpl = Session as Services.Query.Session
                          ?? throw new InvalidOperationException("Underlying session does not support BulkUpsertAsync");

        var resp = await sessionImpl.BulkUpsertAsync(req, cancellationToken).ConfigureAwait(false);
        var status = Status.FromProto(resp.Operation.Status, resp.Operation.Issues);
        status.EnsureSuccess();
    }

    protected override YdbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        ThrowIfConnectionClosed();

        return BeginTransaction(isolationLevel switch
        {
            Serializable or Unspecified => TxMode.SerializableRw,
            _ => throw new ArgumentException("Unsupported isolationLevel: " + isolationLevel)
        });
    }

    public new YdbTransaction BeginTransaction(IsolationLevel isolationLevel) => BeginDbTransaction(isolationLevel);

    public YdbTransaction BeginTransaction(TxMode txMode = TxMode.SerializableRw)
    {
        ThrowIfConnectionClosed();

        if (CurrentTransaction is { Completed: false })
        {
            throw new InvalidOperationException(
                "A transaction is already in progress; nested/concurrent transactions aren't supported."
            );
        }

        CurrentTransaction = new YdbTransaction(this, txMode);

        return CurrentTransaction;
    }

    public override void ChangeDatabase(string databaseName)
    {
    }

    public override void Close() => CloseAsync().GetAwaiter().GetResult();

    public override void Open() => OpenAsync().GetAwaiter().GetResult();

    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        ThrowIfConnectionOpen();

        // Получаем сессию через PoolManager, приводим к ISession (интерфейс)
        Session = await PoolManager.GetSession(ConnectionStringBuilder, cancellationToken);

        OnStateChange(ClosedToOpenEventArgs);
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

            if (CurrentTransaction is { Completed: false })
            {
                await CurrentTransaction.RollbackAsync();
            }

            OnStateChange(OpenToClosedEventArgs);
            ConnectionState = ConnectionState.Closed;
        }
        finally
        {
            if (_session is Services.Query.Session realSession)
                await realSession.Release();
            else
                _session.Close();
        }
    }

    public override string ConnectionString
    {
        get => _connectionStringBuilder?.ConnectionString ?? string.Empty;
#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
        set
#pragma warning restore CS8765
        {
            ThrowIfConnectionOpen();
            _connectionStringBuilder = value != null ? new YdbConnectionStringBuilder(value) : null;
        }
    }

    public override string Database => _connectionStringBuilder?.Database ?? string.Empty;

    public override ConnectionState State => ConnectionState;

    private ConnectionState ConnectionState { get; set; } = ConnectionState.Closed; // Invoke AsyncOpen()

    internal void OnNotSuccessStatusCode(StatusCode code)
    {
        _session.OnNotSuccessStatusCode(code);

        // Проверяем статус сессии: если есть IsActive или IsBroken — корректно переключаем состояние подключения
        if (_session is Services.Query.Session sessionImpl)
        {
            if (!sessionImpl.IsActive)
                ConnectionState = ConnectionState.Broken;
        }
        else if (_session.IsBroken)
        {
            ConnectionState = ConnectionState.Broken;
        }
    }

    internal YdbDataReader? LastReader { get; set; }
    internal string LastCommand { get; set; } = string.Empty;
    internal bool IsBusy => LastReader is { IsOpen: true };
    internal YdbTransaction? CurrentTransaction { get; private set; }

    public override string DataSource => string.Empty; // TODO: DataSource

    public override string ServerVersion
    {
        get
        {
            ThrowIfConnectionClosed();
            return string.Empty; // TODO: ServerVersion
        }
    }

    protected override YdbCommand CreateDbCommand() => new(this);

    public new YdbCommand CreateCommand() => CreateDbCommand();

    public override DataTable GetSchema() => GetSchemaAsync().GetAwaiter().GetResult();

    public override DataTable GetSchema(string collectionName) =>
        GetSchemaAsync(collectionName).GetAwaiter().GetResult();

    public override DataTable GetSchema(string collectionName, string?[] restrictionValues) =>
        GetSchemaAsync(collectionName, restrictionValues).GetAwaiter().GetResult();

    public override Task<DataTable> GetSchemaAsync(CancellationToken cancellationToken = default) =>
        GetSchemaAsync("MetaDataCollections", cancellationToken);

    public override Task<DataTable>
        GetSchemaAsync(string collectionName, CancellationToken cancellationToken = default) =>
        GetSchemaAsync(collectionName, new string[4], cancellationToken);

    public override Task<DataTable> GetSchemaAsync(
        string collectionName,
        string?[] restrictionValues,
        CancellationToken cancellationToken = default
    ) => YdbSchema.GetSchemaAsync(this, collectionName, restrictionValues, cancellationToken);

    internal void ThrowIfConnectionClosed()
    {
        if (ConnectionState is ConnectionState.Closed or ConnectionState.Broken)
        {
            throw new InvalidOperationException("Connection is closed");
        }
    }

    private void ThrowIfConnectionOpen()
    {
        if (ConnectionState == ConnectionState.Open)
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
    /// DB provider factory.
    /// </summary>
    protected override DbProviderFactory DbProviderFactory => YdbProviderFactory.Instance;

    /// <summary>
    /// Clears the connection pool. All idle physical connections in the pool of the given connection are
    /// immediately closed, and any busy connections which were opened before <see cref="ClearPool"/> was called
    /// will be closed when returned to the pool.
    /// </summary>
    public static Task ClearPool(YdbConnection connection) => PoolManager.ClearPool(connection.ConnectionString);

    /// <summary>
    /// Clear all connection pools. All idle physical connections in all pools are immediately closed, and any busy
    /// connections which were opened before <see cref="ClearAllPools"/> was called will be closed when returned
    /// to their pool.
    /// </summary>
    public static Task ClearAllPools() => PoolManager.ClearAllPools();
}