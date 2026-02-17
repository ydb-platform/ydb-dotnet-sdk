using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Tracing;

namespace Ydb.Sdk.Ado;

/// <summary>
/// Represents a SQL command to execute against a YDB database. This class cannot be inherited.
/// </summary>
/// <remarks>
/// YdbCommand provides a standard ADO.NET command interface for executing SQL statements
/// against YDB databases. It supports both synchronous and asynchronous execution methods.
/// </remarks>
public sealed class YdbCommand : DbCommand
{
    private YdbConnection? _ydbConnection;
    private string _commandText = string.Empty;

    private YdbConnection YdbConnection =>
        _ydbConnection ?? throw new InvalidOperationException("Connection property has not been initialized");

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbCommand"/> class.
    /// </summary>
    public YdbCommand()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbCommand"/> class with the specified connection.
    /// </summary>
    /// <param name="ydbConnection">A <see cref="YdbConnection"/> that represents the connection to a YDB server.</param>
    public YdbCommand(YdbConnection ydbConnection)
    {
        _ydbConnection = ydbConnection;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbCommand"/> class with the text of the query, a
    /// <see cref="YdbConnection"/>, and the <see cref="YdbTransaction"/>.
    /// </summary>
    /// <param name="commandText">The text of the query.</param>
    /// <param name="ydbConnection">A <see cref="YdbConnection"/> that represents the connection to a YDB server.</param>
    public YdbCommand(string commandText, YdbConnection? ydbConnection = null)
    {
        _commandText = commandText;
        _ydbConnection = ydbConnection;
    }

    public override void Cancel()
    {
    }

    public override int ExecuteNonQuery() => ExecuteNonQueryAsync().GetAwaiter().GetResult();

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        await using var dataReader = await ExecuteReaderAsync(cancellationToken);

        while (await dataReader.NextResultAsync(cancellationToken))
        {
        }

        return dataReader.RecordsAffected;
    }

    public override object? ExecuteScalar() => ExecuteScalarAsync().GetAwaiter().GetResult();

    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        await using var dataReader = await ExecuteReaderAsync(CommandBehavior.Default, cancellationToken);

        var data = await dataReader.ReadAsync(cancellationToken) ? dataReader.GetValue(0) : null;

        while (await dataReader.NextResultAsync(cancellationToken))
        {
        }

        return data;
    }

    public override void Prepare()
    {
        if (YdbConnection.State == ConnectionState.Closed)
        {
            throw new InvalidOperationException("Connection is not open");
        }

        if (CommandText.Length == 0)
        {
            throw new InvalidOperationException("CommandText property has not been initialized");
        }

        if (YdbConnection.IsBusy)
        {
            throw new YdbOperationInProgressException(YdbConnection);
        }
    }

    public override string CommandText
    {
        get => _commandText;
#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
        [param: AllowNull]
        set
        {
            if (_ydbConnection?.IsBusy ?? false)
            {
                throw new InvalidOperationException("An open data reader exists for this command");
            }

            // ReSharper disable once NullCoalescingConditionIsAlwaysNotNullAccordingToAPIContract
            _commandText = value ?? string.Empty;
        }
#pragma warning restore CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
    }

    public override int CommandTimeout
    {
        get => _timeout;
        set
        {
            if (value < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), value, "CommandTimeout can't be less than zero.");
            }

            _timeout = value;
        }
    }

    private int _timeout;

    public override CommandType CommandType { get; set; } = CommandType.Text;
    public override UpdateRowSource UpdatedRowSource { get; set; }

    protected override DbConnection? DbConnection
    {
        get => _ydbConnection;
        set
        {
            if (_ydbConnection?.IsBusy ?? false)
            {
                throw new InvalidOperationException("An open data reader exists for this command.");
            }

            if (value is null or Ado.YdbConnection)
            {
                _ydbConnection = (YdbConnection?)value;
            }
            else
            {
                throw new ArgumentException(
                    $"Unsupported DbConnection type: {value.GetType()}, expected: {typeof(YdbConnection)}");
            }
        }
    }

    protected override YdbParameterCollection DbParameterCollection { get; } = new();

    public new YdbParameterCollection Parameters => DbParameterCollection;

    protected override DbTransaction? DbTransaction
    {
        get => Transaction;
        set
        {
            if (value is null or YdbTransaction)
            {
                Transaction = (YdbTransaction?)value;
            }
            else
            {
                throw new ArgumentException(
                    $"Unsupported DbTransaction type: {value.GetType()}, expected: {typeof(YdbTransaction)}");
            }
        }
    }

    public new YdbTransaction? Transaction { get; set; }

    public override bool DesignTimeVisible { get; set; }

    protected override YdbParameter CreateDbParameter() => new();

    public new YdbDataReader ExecuteReader(CommandBehavior behavior = CommandBehavior.Default) =>
        ExecuteDbDataReader(behavior);

    protected override YdbDataReader ExecuteDbDataReader(CommandBehavior behavior) =>
        ExecuteReaderAsync(behavior).GetAwaiter().GetResult();

    protected override async Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior,
        CancellationToken cancellationToken)
    {
        var dbActivity = YdbActivitySource.StartActivity("ydb.ExecuteQuery");
        try
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (YdbConnection.IsBusy)
            {
                throw new YdbOperationInProgressException(YdbConnection);
            }

            YdbConnection.ThrowIfConnectionClosed();

            var ydbParameterCollection = DbParameterCollection.YdbParameters;
            var (sql, sqlParams) = SqlParser.Parse(
                CommandText.Length > 0
                    ? CommandText
                    : throw new InvalidOperationException("CommandText property has not been initialized")
            );
            var preparedSql = new StringBuilder();
            var ydbParameters = new Dictionary<string, TypedValue>();

            foreach (var sqlParam in sqlParams)
            {
                if (sqlParam.IsNative && !ydbParameterCollection.ContainsKey(sqlParam.Name))
                {
                    continue;
                }

                var typedValue = sqlParam.YdbValueFetch(ydbParameterCollection);

                if (!sqlParam.IsNative)
                {
                    preparedSql.Append($"DECLARE {sqlParam.Name} AS {typedValue.ToYql()};\n");
                }

                ydbParameters[sqlParam.Name] = typedValue;
            }

            preparedSql.Append(sql);

            var execSettings = CommandTimeout > 0
                ? new GrpcRequestSettings
                    { TransportTimeout = TimeSpan.FromSeconds(CommandTimeout), DbActivity = dbActivity }
                : new GrpcRequestSettings { DbActivity = dbActivity };

            var transaction = YdbConnection.CurrentTransaction;

            if (Transaction != null && Transaction != transaction) // assert on legacy DbTransaction property
            {
                throw new InvalidOperationException("Transaction mismatched! (Maybe using another connection)");
            }

            var ydbDataReader = await YdbDataReader.CreateYdbDataReader(await YdbConnection.Session.ExecuteQuery(
                preparedSql.ToString(), ydbParameters, execSettings, transaction?.TransactionControl
            ), YdbConnection.OnNotSuccessStatusCode, transaction, dbActivity, cancellationToken);

            YdbConnection.LastReader = ydbDataReader;
            YdbConnection.LastCommand = CommandText;

            return ydbDataReader;
        }
        catch (Exception e)
        {
            dbActivity?.SetException(e);
            dbActivity?.Dispose();

            throw;
        }
    }

    public new async Task<YdbDataReader> ExecuteReaderAsync() =>
        (YdbDataReader)await ExecuteDbDataReaderAsync(CommandBehavior.Default, CancellationToken.None);

    public new Task<YdbDataReader> ExecuteReaderAsync(CancellationToken cancellationToken) =>
        ExecuteReaderAsync(CommandBehavior.Default, cancellationToken);

    // ReSharper disable once MemberCanBePrivate.Global
    public new Task<YdbDataReader> ExecuteReaderAsync(CommandBehavior behavior) =>
        ExecuteReaderAsync(behavior, CancellationToken.None);

    // ReSharper disable once MemberCanBePrivate.Global
    public new async Task<YdbDataReader> ExecuteReaderAsync(CommandBehavior behavior,
        CancellationToken cancellationToken) =>
        (YdbDataReader)await ExecuteDbDataReaderAsync(behavior, cancellationToken);
}
