using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Services.Query;

namespace Ydb.Sdk.Ado;

public sealed class YdbCommand : DbCommand
{
    private YdbConnection? _ydbConnection;
    private string _commandText = string.Empty;

    private YdbConnection YdbConnection =>
        _ydbConnection ?? throw new InvalidOperationException("Connection property has not been initialized");

    public YdbCommand()
    {
    }

    public YdbCommand(YdbConnection ydbConnection)
    {
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
        if (YdbConnection.IsBusy)
        {
            throw new YdbOperationInProgressException(YdbConnection);
        }

        YdbConnection.EnsureConnectionOpen();

        var ydbParameters = DbParameterCollection.YdbParameters;
        var (sql, paramNames) = SqlParser.Parse(
            CommandText.Length > 0
                ? CommandText
                : throw new InvalidOperationException("CommandText property has not been initialized")
        );
        var preparedSql = new StringBuilder();

        foreach (var paramName in paramNames)
        {
            if (ydbParameters.TryGetValue(paramName, out var ydbValue))
            {
                preparedSql.Append($"DECLARE {paramName} AS {ydbValue.ToYql};\n");
            }
            else
            {
                throw new YdbException($"Not found YDB parameter [name: {paramName}]");
            }
        }

        preparedSql.Append(sql);

        var execSettings = CommandTimeout > 0
            ? new ExecuteQuerySettings { TransportTimeout = TimeSpan.FromSeconds(CommandTimeout) }
            : new ExecuteQuerySettings();
        execSettings.CancellationToken = cancellationToken;

        var transaction = YdbConnection.CurrentTransaction;

        if (Transaction != null && Transaction != transaction) // assert on legacy DbTransaction property
        {
            throw new InvalidOperationException("Transaction mismatched! (Maybe using another connection)");
        }

        var ydbDataReader = await YdbDataReader.CreateYdbDataReader(
            await YdbConnection.Session.ExecuteQuery(
                preparedSql.ToString(), ydbParameters, execSettings, transaction?.TransactionControl
            ),
            YdbConnection.Session.OnStatus, transaction
        );

        YdbConnection.LastReader = ydbDataReader;
        YdbConnection.LastCommand = CommandText;

        return ydbDataReader;
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
