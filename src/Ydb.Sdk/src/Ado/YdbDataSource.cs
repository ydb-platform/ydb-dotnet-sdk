#if NET7_0_OR_GREATER

using System.Data.Common;

namespace Ydb.Sdk.Ado;

public class YdbDataSource : DbDataSource
{
    private readonly YdbConnectionStringBuilder _ydbConnectionStringBuilder;

    public YdbDataSource(YdbConnectionStringBuilder connectionStringBuilder)
    {
        _ydbConnectionStringBuilder = connectionStringBuilder;
    }

    public YdbDataSource(string connectionString)
    {
        _ydbConnectionStringBuilder = new YdbConnectionStringBuilder(connectionString);
    }

    public YdbDataSource()
    {
        _ydbConnectionStringBuilder = new YdbConnectionStringBuilder();
    }

    protected override YdbConnection CreateDbConnection()
    {
        return new YdbConnection(_ydbConnectionStringBuilder);
    }

    protected override YdbConnection OpenDbConnection()
    {
        var dbConnection = CreateDbConnection();
        try
        {
            dbConnection.Open();
            return dbConnection;
        }
        catch
        {
            dbConnection.Close();
            throw;
        }
    }

    public new YdbConnection CreateConnection() => CreateDbConnection();

    public new YdbConnection OpenConnection() => OpenDbConnection();

    public new async ValueTask<YdbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        var ydbConnection = CreateDbConnection();

        try
        {
            await ydbConnection.OpenAsync(cancellationToken);
            return ydbConnection;
        }
        catch
        {
            await ydbConnection.CloseAsync();
            throw;
        }
    }

    public override string ConnectionString => _ydbConnectionStringBuilder.ConnectionString;
}

#endif
