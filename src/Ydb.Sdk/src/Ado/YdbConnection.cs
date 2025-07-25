using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Ado.Session;
using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Value;
using Ydb.Table;
using Ydb.Table.V1;
using static System.Data.IsolationLevel;

namespace Ydb.Sdk.Ado;

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

    public YdbConnection()
    {
    }

    public YdbConnection(string connectionString)
    {
        ConnectionStringBuilder = new YdbConnectionStringBuilder(connectionString);
    }

    public YdbConnection(YdbConnectionStringBuilder connectionStringBuilder)
    {
        ConnectionStringBuilder = connectionStringBuilder;
    }

    public async Task BulkUpsertAsync(
        string tablePath,
        IReadOnlyList<string> columns,
        IReadOnlyList<IReadOnlyList<object?>> rows,
        CancellationToken cancellationToken = default)
    {
        if (CurrentTransaction is { Completed: false })
            throw new InvalidOperationException("BulkUpsert cannot be used inside an active transaction.");

        if (columns == null || columns.Count == 0)
            throw new ArgumentException("Columns must not be empty", nameof(columns));
        if (rows == null || rows.Count == 0)
            throw new ArgumentException("Rows collection is empty", nameof(rows));

        var structs = rows.Select(row =>
        {
            if (row.Count != columns.Count)
                throw new ArgumentException("Each row must have the same number of elements as columns");
            var members = columns
                .Select((col, i) =>
                    new KeyValuePair<string, YdbValue>(col, new YdbParameter { Value = row[i] }.YdbValue))
                .ToDictionary(x => x.Key, x => x.Value);
            return YdbValue.MakeStruct(members);
        }).ToList();

        var list = YdbValue.MakeList(structs);

        var req = new BulkUpsertRequest
        {
            Table = tablePath,
            Rows = list.GetProto()
        };

        var resp = await Session.Driver.UnaryCall(
            TableService.BulkUpsertMethod,
            req,
            new GrpcRequestSettings { CancellationToken = cancellationToken }
        ).ConfigureAwait(false);

        var operation = resp.Operation;
        if (operation.Status.IsNotSuccess())
        {
            throw YdbException.FromServer(operation.Status, operation.Issues);
        }
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
        try
        {
            Session = await PoolManager.GetSession(ConnectionStringBuilder, cancellationToken);
        }
        catch (OperationCanceledException e)
        {
            throw new YdbException(StatusCode.Cancelled,
                $"The connection pool has been exhausted, either raise 'MaxSessionPool' " +
                $"(currently {ConnectionStringBuilder.MaxSessionPool}) or 'CreateSessionTimeout' " +
                $"(currently {ConnectionStringBuilder.CreateSessionTimeout} seconds) in your connection string.", e
            );
        }

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
            _session.Close();
        }
    }

    public override string ConnectionString
    {
        get => _connectionStringBuilder?.ConnectionString ?? string.Empty;
#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
        set
#pragma warning restore CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
        {
            ThrowIfConnectionOpen();

            // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
            _connectionStringBuilder = value != null ? new YdbConnectionStringBuilder(value) : null;
        }
    }

    public override string Database => _connectionStringBuilder?.Database ?? string.Empty;

    public override ConnectionState State => ConnectionState;

    private ConnectionState ConnectionState { get; set; } = ConnectionState.Closed; // Invoke AsyncOpen()

    internal void OnNotSuccessStatusCode(StatusCode code)
    {
        _session.OnNotSuccessStatusCode(code);

        if (_session.IsBroken)
        {
            ConnectionState = ConnectionState.Broken;
        }
    }

    internal YdbDataReader? LastReader { get; set; }
    internal string LastCommand { get; set; } = string.Empty;
    internal bool IsBusy => LastReader is { IsOpen: true };
    internal YdbTransaction? CurrentTransaction { get; private set; }

    public override string DataSource => string.Empty; // TODO

    public override string ServerVersion
    {
        get
        {
            ThrowIfConnectionClosed();

            return string.Empty; // TODO ServerVersion
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
