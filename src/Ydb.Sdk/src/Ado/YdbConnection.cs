using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Ydb.Sdk.Ado;

public sealed class YdbConnection : DbConnection
{
    private readonly YdbContext _ydbContext;

    internal IYdbConnectionState YdbConnectionState { get; private set; }

    internal void NextOutTransactionState()
    {
        YdbConnectionState = new OutTransactionState(); // TODO
    }

    internal YdbConnection(YdbContext ydbContext)
    {
        _ydbContext = ydbContext;
        YdbConnectionState = new OutTransactionState();
    }

    protected override YdbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
    {
        throw new NotImplementedException();
    }

    public override void ChangeDatabase(string databaseName)
    {
    }

    public override void Close()
    {
        throw new NotImplementedException();
    }

    public override void Open()
    {
        throw new NotImplementedException();
    }

    public override string ConnectionString
    {
        get => _ydbContext.ConnectionString;
        [param: AllowNull]
        set
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
        } // TODO
    }

    public override string Database => _ydbContext.Database;

    public override ConnectionState State => ConnectionState.Open; // TODO
    public override string DataSource => string.Empty; // TODO
    public override string ServerVersion => string.Empty; // TODO

    protected override YdbCommand CreateDbCommand()
    {
        return new YdbCommand();
    }
}
