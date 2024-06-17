using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Ydb.Sdk.Ado;

public sealed class YdbCommand : DbCommand
{
    private string _commandText = string.Empty;

    internal YdbCommand()
    {
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
        [param: AllowNull]
        set
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

    protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
    {
        throw new NotImplementedException();
    }
}
