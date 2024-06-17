using System.Data;
using System.Data.Common;

namespace Ydb.Sdk.Ado;

public sealed class YdbCommand : DbCommand
{
    private readonly YdbConnection _ydbConnection;

    private string _commandText;

    private IsolationLevel IsolationLevel { get; set; } = IsolationLevel.Serializable;

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
        set
        {
            _commandText = value ?? string.Empty;
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

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        throw new NotImplementedException();
    }
}
