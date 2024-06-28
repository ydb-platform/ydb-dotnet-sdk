using System.Data;
using System.Data.Common;
using Ydb.Sdk.Services.Query;

namespace Ydb.Sdk.Ado;

public sealed class YdbCommand : DbCommand
{
    private readonly YdbConnection _ydbConnection;

    private string _commandText = string.Empty;

    internal YdbCommand(YdbConnection ydbConnection)
    {
        _ydbConnection = ydbConnection;
    }

    public override void Cancel()
    {
        throw new NotImplementedException();
    }

    public override int ExecuteNonQuery()
    {
        throw new NotImplementedException();
    }

    public override object ExecuteScalar()
    {
        throw new NotImplementedException();
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

    public override int CommandTimeout { get; set; }
    public override CommandType CommandType { get; set; } = CommandType.Text;
    public override UpdateRowSource UpdatedRowSource { get; set; }
    protected override DbConnection? DbConnection { get; set; }

    protected override YdbParameterCollection DbParameterCollection { get; } = new();

    protected override DbTransaction? DbTransaction { get; set; }
    public override bool DesignTimeVisible { get; set; }

    protected override YdbParameter CreateDbParameter()
    {
        return new YdbParameter();
    }

    protected override YdbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        var execSettings = CommandTimeout > 0
            ? new ExecuteQuerySettings { TransportTimeout = TimeSpan.FromSeconds(CommandTimeout) }
            : ExecuteQuerySettings.DefaultInstance;

        var ydbDataReader = new YdbDataReader(_ydbConnection.ExecuteQuery(_commandText,
            DbParameterCollection.YdbParameters, execSettings, (YdbTransaction?)DbTransaction));

        _ydbConnection.CurrentReader = ydbDataReader;

        return ydbDataReader;
    }
}
