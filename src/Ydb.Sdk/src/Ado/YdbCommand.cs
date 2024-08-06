using System.Data;
using System.Data.Common;
using Ydb.Sdk.Services.Query;

namespace Ydb.Sdk.Ado;

public sealed class YdbCommand : DbCommand
{
    private YdbConnection YdbConnection { get; set; }

    private string _commandText = string.Empty;

    public YdbCommand(YdbConnection ydbConnection)
    {
        YdbConnection = ydbConnection;
    }

    public override void Cancel()
    {
        throw new NotImplementedException("Currently not supported");
    }

    public override int ExecuteNonQuery()
    {
        return ExecuteNonQueryAsync().GetAwaiter().GetResult();
    }

    public override async Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
    {
        await using var dataReader = ExecuteReader();

        if (!await dataReader.NextResultAsync(cancellationToken))
        {
            throw new YdbException("YDB server closed the stream");
        }

        while (await dataReader.ReadAsync(cancellationToken))
        {
        }

        return dataReader.RecordsAffected;
    }

    public override object? ExecuteScalar()
    {
        return ExecuteScalarAsync().GetAwaiter().GetResult();
    }

    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        await using var dataReader = ExecuteDbDataReader(CommandBehavior.Default);

        var data = await dataReader.ReadAsync(cancellationToken)
            ? dataReader.IsDBNull(0) ? null : dataReader.GetValue(0)
            : null;

        while (await dataReader.ReadAsync(cancellationToken))
        {
        }

        return data;
    }

    public override void Prepare()
    {
        // Do nothing
    }

    public override string CommandText
    {
        get => _commandText;
#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
        set
#pragma warning restore CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
        {
            _commandText = value ?? throw new ArgumentNullException(nameof(value));
            DbParameterCollection.Clear();
        }
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
        get => YdbConnection;
        set
        {
            if (value is YdbConnection ydbConnection)
            {
                YdbConnection = ydbConnection;
            }
            else
            {
                throw new ArgumentException(
                    $"Unsupported DbTransaction type: {value?.GetType()}, expected: {typeof(YdbConnection)}");
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
                _ydbTransaction = (YdbTransaction?)value;
            }
            else
            {
                throw new ArgumentException(
                    $"Unsupported DbTransaction type: {value.GetType()}, expected: {typeof(YdbTransaction)}");
            }
        }
    }

    public new YdbTransaction? Transaction
    {
        get
        {
            if (_ydbTransaction?.Completed ?? false)
            {
                _ydbTransaction = null;
            }

            return _ydbTransaction;
        }
        set => _ydbTransaction = value;
    }

    private YdbTransaction? _ydbTransaction;

    public override bool DesignTimeVisible { get; set; }

    protected override YdbParameter CreateDbParameter()
    {
        return new YdbParameter();
    }

    public new YdbDataReader ExecuteReader(CommandBehavior behavior = CommandBehavior.Default)
    {
        return ExecuteDbDataReader(behavior);
    }

    protected override YdbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        if (YdbConnection.LastReader is { IsClosed: false })
        {
            throw new YdbOperationInProgressException(YdbConnection);
        }

        var execSettings = CommandTimeout > 0
            ? new ExecuteQuerySettings { TransportTimeout = TimeSpan.FromSeconds(CommandTimeout) }
            : ExecuteQuerySettings.DefaultInstance;

        var ydbDataReader = new YdbDataReader(YdbConnection.Session.ExecuteQuery(_commandText,
            DbParameterCollection.YdbParameters, execSettings, Transaction?.TransactionControl), Transaction);

        YdbConnection.LastReader = ydbDataReader;
        YdbConnection.LastCommand = CommandText;
        YdbConnection.LastTransaction = Transaction;

        return ydbDataReader;
    }
}
