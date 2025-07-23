#if NET7_0_OR_GREATER
using System.Data.Common;
using Ydb.Sdk.Ado.BulkUpsert;
using Ydb.Sdk.Services.Table;

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

    public override string ConnectionString => _ydbConnectionStringBuilder.ConnectionString;

    protected override async ValueTask DisposeAsyncCore() =>
        await PoolManager.ClearPool(_ydbConnectionStringBuilder.ConnectionString);

    protected override void Dispose(bool disposing) => DisposeAsyncCore().AsTask().GetAwaiter().GetResult();
    
    public async Task<YdbBulkUpsertImporter<T>> BeginBulkUpsertAsync<T>(
        string tablePath,
        BulkUpsertOptions? options = null,
        RetrySettings? retrySettings = null,
        int maxBatchSizeBytes = 64 * 1024 * 1024,
        CancellationToken cancellationToken = default)
    {
        var conn = await OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

        var realSession = conn.Session as Services.Query.Session
                          ?? throw new InvalidOperationException("Underlying session does not support bulk upsert");

        var driver = realSession.Driver as Driver
                     ?? throw new InvalidOperationException("Session driver is not of expected type 'Ydb.Sdk.Driver'");

        var tableClient = new TableClient(driver);

        return new YdbBulkUpsertImporter<T>(tableClient, tablePath, options, retrySettings, maxBatchSizeBytes);
    }
}

#endif
