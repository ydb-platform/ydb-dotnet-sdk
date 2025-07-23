#if NET7_0_OR_GREATER
using System.Data.Common;
using Ydb.Sdk.Ado.BulkUpsert;

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

    protected override YdbConnection CreateDbConnection() => new(_ydbConnectionStringBuilder);

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
    
    public async Task<YdbBulkUpsertImporter<T>> BeginBulkUpsertAsync<T>(
        string tablePath,
        BulkUpsertOptions? options = null,
        int maxBatchSizeBytes = 64 * 1024 * 1024,
        CancellationToken cancellationToken = default)
    {
        var conn = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return new YdbBulkUpsertImporter<T>(conn, tablePath, options, maxBatchSizeBytes);
    }

    public override string ConnectionString => _ydbConnectionStringBuilder.ConnectionString;

    protected override async ValueTask DisposeAsyncCore() =>
        await PoolManager.ClearPool(_ydbConnectionStringBuilder.ConnectionString);

    protected override void Dispose(bool disposing) => DisposeAsyncCore().AsTask().GetAwaiter().GetResult();
}

#endif
