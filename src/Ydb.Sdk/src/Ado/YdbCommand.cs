using System.Data;
using System.Data.Common;
using Ydb.Sdk.Services.Query;

namespace Ydb.Sdk.Ado;

public sealed class YdbCommand : DbCommand
{
    private YdbConnection YdbConnection { get; set; }

    private string _commandText = string.Empty;

    internal YdbCommand(YdbConnection ydbConnection)
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
        await using var dataReader = ExecuteDbDataReader(CommandBehavior.Default);

        if (await dataReader.NextResultAsync(cancellationToken))
        {
            return dataReader.RecordsAffected;
        }

        throw new YdbException("YDB server closed the stream");
    }

    public override object? ExecuteScalar()
    {
        return ExecuteScalarAsync().GetAwaiter().GetResult();
    }

    public override async Task<object?> ExecuteScalarAsync(CancellationToken cancellationToken)
    {
        await using var dataReader = ExecuteDbDataReader(CommandBehavior.Default);

        return await dataReader.ReadAsync(cancellationToken)
            ? dataReader.IsDBNull(0) ? null : dataReader.GetValue(0)
            : null;
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

    protected override DbTransaction? DbTransaction
    {
        get => _ydbTransaction;
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

    private YdbTransaction? _ydbTransaction;

    public override bool DesignTimeVisible { get; set; }

    protected override YdbParameter CreateDbParameter()
    {
        return new YdbParameter();
    }

    protected override YdbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        if (YdbConnection.CurrentReader != null)
        {
            throw new InvalidOperationException(
                "There is already an open YdbDataReader. Check if the previously opened YdbDataReader has been closed.");
        }

        var execSettings = CommandTimeout > 0
            ? new ExecuteQuerySettings { TransportTimeout = TimeSpan.FromSeconds(CommandTimeout) }
            : ExecuteQuerySettings.DefaultInstance;

        var ydbDataReader = new YdbDataReader(YdbConnection.ExecuteQuery(_commandText,
            DbParameterCollection.YdbParameters, execSettings, _ydbTransaction));

        YdbConnection.CurrentReader = ydbDataReader;

        return ydbDataReader;
    }
}
