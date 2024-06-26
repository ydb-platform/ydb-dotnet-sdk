using System.Data;
using System.Data.Common;

namespace Ydb.Sdk.Ado;

public sealed class YdbConnection : DbConnection
{
    private readonly YdbDataSource _ydbDataSource;

    internal IYdbConnectionState YdbConnectionState { get; private set; }

    internal void NextOutTransactionState()
    {
        YdbConnectionState = new OutTransactionState(_ydbDataSource); // TODO
    }

    internal YdbConnection(YdbDataSource ydbDataSource)
    {
        _ydbDataSource = ydbDataSource;
        YdbConnectionState = new OutTransactionState(_ydbDataSource);
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
        get => _ydbDataSource.ConnectionString;
#pragma warning disable CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
        set
#pragma warning restore CS8765 // Nullability of type of parameter doesn't match overridden member (possibly because of nullability attributes).
        {
            if (value == null) throw new ArgumentNullException(nameof(value));
        } // TODO
    }

    public override string Database => _ydbDataSource.Database;

    public override ConnectionState State => ConnectionState.Open; // TODO
    public override string DataSource => string.Empty; // TODO
    public override string ServerVersion => string.Empty; // TODO

    protected override YdbCommand CreateDbCommand()
    {
        return new YdbCommand();
    }
}
