using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Ado.BulkUpsert;
using Ydb.Sdk.Ado.RetryPolicy;
using Ydb.Sdk.Ado.Session;
using Ydb.Sdk.Ado.TxWriter;
using Ydb.Sdk.Topic;
using static System.Data.IsolationLevel;

namespace Ydb.Sdk.Ado;

/// <summary>
/// Represents a connection to a YDB database.
/// </summary>
/// <remarks>
/// YdbConnection provides a standard ADO.NET connection interface for YDB databases.
/// It manages database sessions and provides access to YDB-specific functionality.
/// </remarks>
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

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbConnection"/> class.
    /// </summary>
    public YdbConnection()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbConnection"/> class with the specified connection string.
    /// </summary>
    /// <param name="connectionString">The connection string used to establish the connection.</param>
    public YdbConnection(string connectionString)
    {
        ConnectionStringBuilder = new YdbConnectionStringBuilder(connectionString);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbConnection"/> class with the specified connection string builder.
    /// </summary>
    /// <param name="connectionStringBuilder">
    /// The <see cref="YdbConnectionStringBuilder"/> used to establish the connection.
    /// </param>
    public YdbConnection(YdbConnectionStringBuilder connectionStringBuilder)
    {
        ConnectionStringBuilder = connectionStringBuilder;
    }

    /// <summary>
    /// Begins a database transaction with the specified isolation level.
    /// </summary>
    /// <remarks>
    /// Maps the requested ADO.NET <see cref="IsolationLevel"/> to a YDB
    /// <see cref="TransactionMode"/>:
    /// <list type="bullet">
    ///   <item>
    ///     <description>
    ///       <see cref="IsolationLevel.Serializable"/> or
    ///       <see cref="IsolationLevel.Unspecified"/> →
    ///       <see cref="TransactionMode.SerializableRw"/>
    ///     </description>
    ///   </item>
    ///   <item>
    ///     <description>
    ///       <see cref="IsolationLevel.Snapshot"/> or
    ///       <see cref="IsolationLevel.RepeatableRead"/> →
    ///       <see cref="TransactionMode.SnapshotRw"/>
    ///     </description>
    ///   </item>
    /// </list>
    ///
    /// The <see cref="TransactionMode.SnapshotRw"/> mode in YDB provides snapshot
    /// isolation with optimistic concurrency: if there is a concurrent write
    /// conflict, the transaction may be aborted by the server. This behavior is similar to
    /// <see cref="IsolationLevel.Snapshot"/> in ADO.NET.
    /// </remarks>
    protected override YdbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        ThrowIfConnectionClosed();

        return BeginTransaction(isolationLevel switch
        {
            Serializable or Unspecified => TransactionMode.SerializableRw,
            Snapshot or RepeatableRead => TransactionMode.SnapshotRw,
            _ => throw new ArgumentException("Unsupported isolationLevel: " + isolationLevel)
        });
    }

    public new YdbTransaction BeginTransaction(IsolationLevel isolationLevel) => BeginDbTransaction(isolationLevel);

    public YdbTransaction BeginTransaction(TransactionMode transactionMode = TransactionMode.SerializableRw)
    {
        ThrowIfConnectionClosed();

        if (CurrentTransaction is { Completed: false })
        {
            throw new InvalidOperationException(
                "A transaction is already in progress; nested/concurrent transactions aren't supported."
            );
        }

        CurrentTransaction = new YdbTransaction(this, transactionMode);

        return CurrentTransaction;
    }

    public override void ChangeDatabase(string databaseName) => throw new NotSupportedException();

    public override void Close() => CloseAsync().GetAwaiter().GetResult();

    public override void Open() => OpenAsync().GetAwaiter().GetResult();

    public override async Task OpenAsync(CancellationToken cancellationToken)
    {
        ThrowIfConnectionOpen();

        var sessionSource = await PoolManager.Get(ConnectionStringBuilder, cancellationToken);

        Session = await sessionSource.OpenSession(cancellationToken);

        OnStateChange(ClosedToOpenEventArgs);

        ConnectionState = ConnectionState.Open;
    }

    internal async ValueTask OpenAsync(
        YdbRetryPolicyExecutor retryPolicyExecutor,
        CancellationToken cancellationToken = default
    )
    {
        ThrowIfConnectionOpen();

        var sessionSource = await PoolManager.Get(ConnectionStringBuilder, cancellationToken);

        Session = new RetryableSession(sessionSource, retryPolicyExecutor);

        ConnectionState = ConnectionState.Open;
    }

    public override async Task CloseAsync()
    {
        // ReSharper disable once SwitchStatementHandlesSomeKnownEnumValuesWithDefault
        switch (State)
        {
            case ConnectionState.Closed:
                return;
            case ConnectionState.Broken:
                ConnectionState = ConnectionState.Closed;
                _session.Dispose();
                return;
            default:
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
                    _session.Dispose();
                }

                break;
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

    public override ConnectionState State =>
        ConnectionState != ConnectionState.Closed && _session.IsBroken // maybe is updated asynchronously
            ? ConnectionState.Broken
            : ConnectionState;

    private ConnectionState ConnectionState { get; set; } = ConnectionState.Closed; // Invoke AsyncOpen()

    internal void OnNotSuccessStatusCode(StatusCode code) => _session.OnNotSuccessStatusCode(code);

    internal YdbDataReader? LastReader { get; set; }
    internal string LastCommand { get; set; } = string.Empty;
    internal bool IsBusy => LastReader is { IsOpen: true };
    internal YdbTransaction? CurrentTransaction { get; private set; }
    internal List<object> TxWriters { get; } = new();

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
        if (State is ConnectionState.Closed or ConnectionState.Broken)
        {
            throw new InvalidOperationException("Connection is closed");
        }
    }

    private void ThrowIfConnectionOpen()
    {
        if (State == ConnectionState.Open)
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

    /// <summary>
    /// Creates a transactional topic writer that binds message writes to the current transaction.
    /// </summary>
    /// <typeparam name="T">The type of values to write to the topic.</typeparam>
    /// <param name="topicName">The name of the topic to write to.</param>
    /// <param name="options">Optional configuration for the writer.</param>
    /// <returns>A transactional topic writer instance.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when the connection is closed or no transaction is active.
    /// </exception>
    /// <remarks>
    /// <para>
    /// All messages written via the returned writer are bound to the current transaction
    /// and become visible atomically together with table changes after a successful commit.
    /// Message sending is performed in the background while the application continues to work.
    /// </para>
    /// <para>
    /// Before committing, the connection automatically waits for acknowledgements of all pending messages.
    /// If any flush fails, the commit is not attempted and the transaction is rolled back.
    /// </para>
    /// <para>
    /// A transaction must be started (via BeginTransaction) before calling this method.
    /// If no transaction is active, an InvalidOperationException will be thrown.
    /// </para>
    /// </remarks>
    public ITxTopicWriter<T> CreateTxWriter<T>(string topicName, TxWriterOptions? options = null)
    {
        ThrowIfConnectionClosed();

        if (CurrentTransaction is not { Completed: false })
        {
            throw new InvalidOperationException(
                "A transaction must be active to create a transactional topic writer. " +
                "Call BeginTransaction() before creating a TxWriter.");
        }

        options ??= new TxWriterOptions();

        var database = ConnectionStringBuilder.Database.TrimEnd('/');
        var topicPath = topicName.StartsWith(database) ? topicName : $"{database}/{topicName}";

        var serializer = Serializers.DefaultSerializers.TryGetValue(typeof(T), out var defaultSerializer)
            ? (ISerializer<T>)defaultSerializer
            : throw new InvalidOperationException(
                $"No default serializer found for type {typeof(T).Name}. " +
                "Configure a serializer in TxWriterOptions or use a supported type.");

        var logger = Session.Driver.LoggerFactory.CreateLogger<TxTopicWriter<T>>();

        var writer = new TxTopicWriter<T>(
            CurrentTransaction,
            topicPath,
            options,
            serializer,
            logger);

        TxWriters.Add(writer);

        return writer;
    }

    public IBulkUpsertImporter BeginBulkUpsertImport(
        string name,
        IReadOnlyList<string> columns,
        CancellationToken cancellationToken = default)
    {
        ThrowIfConnectionClosed();

        if (CurrentTransaction is { Completed: false })
            throw new InvalidOperationException("BulkUpsert cannot be used inside an active transaction.");

        var database = ConnectionStringBuilder.Database.TrimEnd('/');
        var tablePath = name.StartsWith(database) ? name : $"{database}/{name}";

        var maxBytes = ConnectionStringBuilder.MaxSendMessageSize;

        return new BulkUpsertImporter(Session.Driver, tablePath, columns, maxBytes, cancellationToken);
    }
}
