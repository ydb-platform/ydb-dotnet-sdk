using System.Collections.Concurrent;
using Ydb.Sdk.Ado.RetryPolicy;
#if NET7_0_OR_GREATER
using System.Data.Common;
#endif

namespace Ydb.Sdk.Ado;

public class YdbDataSource
#if NET7_0_OR_GREATER
    : DbDataSource
#else
    : IAsyncDisposable
#endif
{
    private static readonly ConcurrentDictionary<string, YdbRetryPolicyExecutor> CacheYdbRetryPolicyExecutors = new();

    private static YdbRetryPolicyExecutor GetExecutor(YdbRetryPolicyConfig config) =>
        CacheYdbRetryPolicyExecutors.GetOrAdd(config.ToString(),
            new YdbRetryPolicyExecutor(new YdbRetryPolicy(config)));

    private readonly YdbConnectionStringBuilder _ydbConnectionStringBuilder;
    private readonly YdbRetryPolicyExecutor _retryPolicyExecutor;

    public YdbDataSource(YdbConnectionStringBuilder connectionStringBuilder)
    {
        _ydbConnectionStringBuilder = connectionStringBuilder;
        _retryPolicyExecutor = _ydbConnectionStringBuilder.YdbRetryPolicyExecutor;
    }

    public YdbDataSource(string connectionString)
    {
        _ydbConnectionStringBuilder = new YdbConnectionStringBuilder(connectionString);
        _retryPolicyExecutor = _ydbConnectionStringBuilder.YdbRetryPolicyExecutor;
    }

    public YdbDataSource()
    {
        _ydbConnectionStringBuilder = new YdbConnectionStringBuilder();
        _retryPolicyExecutor = _ydbConnectionStringBuilder.YdbRetryPolicyExecutor;
    }

    protected
#if NET7_0_OR_GREATER
        override
#endif
        YdbConnection CreateDbConnection() => new(_ydbConnectionStringBuilder);

    protected
#if NET7_0_OR_GREATER
        override
#endif
        YdbConnection OpenDbConnection()
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

    public
#if NET7_0_OR_GREATER
        new
#endif
        YdbConnection CreateConnection() => CreateDbConnection();

    public
#if NET7_0_OR_GREATER
        new
#endif
        YdbConnection OpenConnection() => OpenDbConnection();

    public
#if NET7_0_OR_GREATER
        new
#endif
        async ValueTask<YdbConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
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

#if NET7_0_OR_GREATER
    public override string ConnectionString => _ydbConnectionStringBuilder.ConnectionString;
#endif

    protected
#if NET7_0_OR_GREATER
    override
#endif
        async ValueTask DisposeAsyncCore() => await PoolManager.ClearPool(_ydbConnectionStringBuilder.ConnectionString);

#if NET7_0_OR_GREATER
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            DisposeAsyncCore().AsTask().GetAwaiter().GetResult();
        }
    }
#else
    public async ValueTask DisposeAsync()
    {
        await DisposeAsyncCore().ConfigureAwait(false);

        GC.SuppressFinalize(this);
    }
#endif

    public Task ExecuteAsync(Func<YdbConnection, Task> func) => _retryPolicyExecutor
        .ExecuteAsync(async cancellationToken =>
        {
            await using var ydbConnection = await OpenConnectionAsync(cancellationToken);
            await func(ydbConnection);
        });

    public Task<TResult> ExecuteAsync<TResult>(Func<YdbConnection, Task<TResult>> func) => _retryPolicyExecutor
        .ExecuteAsync<TResult>(async cancellationToken =>
        {
            await using var ydbConnection = await OpenConnectionAsync(cancellationToken);
            return await func(ydbConnection);
        });

    public Task ExecuteAsync(
        Func<YdbConnection, CancellationToken, Task> func,
        CancellationToken cancellationToken = default
    ) => _retryPolicyExecutor.ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        await func(ydbConnection, ct);
    }, cancellationToken);

    public Task<TResult> ExecuteAsync<TResult>(
        Func<YdbConnection, CancellationToken, Task<TResult>> func,
        CancellationToken cancellationToken = default
    ) => _retryPolicyExecutor.ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        return await func(ydbConnection, ct);
    }, cancellationToken);

    public Task ExecuteAsync(
        Func<YdbConnection, Task> func,
        YdbRetryPolicyConfig retryPolicyConfig
    ) => GetExecutor(retryPolicyConfig).ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        await func(ydbConnection);
    });

    public Task ExecuteAsync(
        Func<YdbConnection, Task> func,
        IRetryPolicy retryPolicy
    ) => new YdbRetryPolicyExecutor(retryPolicy).ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        await func(ydbConnection);
    });

    public Task ExecuteAsync(
        Func<YdbConnection, CancellationToken, Task> func,
        YdbRetryPolicyConfig retryPolicyConfig,
        CancellationToken cancellationToken = default
    ) => GetExecutor(retryPolicyConfig).ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        await func(ydbConnection, ct);
    }, cancellationToken);

    public Task ExecuteAsync(
        Func<YdbConnection, CancellationToken, Task> func,
        IRetryPolicy retryPolicy,
        CancellationToken cancellationToken = default
    ) => new YdbRetryPolicyExecutor(retryPolicy).ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        await func(ydbConnection, ct);
    }, cancellationToken);

    public Task<TResult> ExecuteAsync<TResult>(
        Func<YdbConnection, CancellationToken, Task<TResult>> func,
        YdbRetryPolicyConfig retryPolicyConfig,
        CancellationToken cancellationToken = default
    ) => GetExecutor(retryPolicyConfig).ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        return await func(ydbConnection, ct);
    }, cancellationToken);

    public Task<TResult> ExecuteAsync<TResult>(
        Func<YdbConnection, CancellationToken, Task<TResult>> func,
        IRetryPolicy retryPolicy,
        CancellationToken cancellationToken = default
    ) => new YdbRetryPolicyExecutor(retryPolicy).ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        return await func(ydbConnection, ct);
    }, cancellationToken);

    public Task ExecuteInTransactionAsync(Func<YdbConnection, Task> func,
        TransactionMode transactionMode = TransactionMode.SerializableRw) =>
        _retryPolicyExecutor.ExecuteAsync(async cancellationToken =>
        {
            await using var ydbConnection = await OpenConnectionAsync(cancellationToken);
            await using var transaction = ydbConnection.BeginTransaction(transactionMode);
            await func(ydbConnection);
            await transaction.CommitAsync(cancellationToken);
        });

    public Task<TResult> ExecuteInTransactionAsync<TResult>(
        Func<YdbConnection, Task<TResult>> func,
        TransactionMode transactionMode = TransactionMode.SerializableRw
    ) => _retryPolicyExecutor.ExecuteAsync<TResult>(async cancellationToken =>
    {
        await using var ydbConnection = await OpenConnectionAsync(cancellationToken);
        await using var transaction = ydbConnection.BeginTransaction(transactionMode);
        var result = await func(ydbConnection).ConfigureAwait(false);
        await transaction.CommitAsync(cancellationToken);
        return result;
    });

    public Task ExecuteInTransactionAsync(
        Func<YdbConnection, CancellationToken, Task> func,
        TransactionMode transactionMode = TransactionMode.SerializableRw,
        CancellationToken cancellationToken = default
    ) => _retryPolicyExecutor.ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        await using var transaction = ydbConnection.BeginTransaction(transactionMode);
        await func(ydbConnection, ct);
        await transaction.CommitAsync(ct);
    }, cancellationToken);

    public Task<TResult> ExecuteInTransactionAsync<TResult>(
        Func<YdbConnection, CancellationToken, Task<TResult>> func,
        TransactionMode transactionMode = TransactionMode.SerializableRw,
        CancellationToken cancellationToken = default
    ) => _retryPolicyExecutor.ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        await using var transaction = ydbConnection.BeginTransaction(transactionMode);
        var result = await func(ydbConnection, ct);
        await transaction.CommitAsync(ct);
        return result;
    }, cancellationToken);

    public Task ExecuteInTransactionAsync(
        Func<YdbConnection, CancellationToken, Task> func,
        YdbRetryPolicyConfig retryPolicyConfig,
        TransactionMode transactionMode = TransactionMode.SerializableRw,
        CancellationToken cancellationToken = default
    ) => GetExecutor(retryPolicyConfig).ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        await using var transaction = ydbConnection.BeginTransaction(transactionMode);
        await func(ydbConnection, ct);
        await transaction.CommitAsync(ct);
    }, cancellationToken);

    public Task<TResult> ExecuteInTransactionAsync<TResult>(
        Func<YdbConnection, CancellationToken, Task<TResult>> func,
        YdbRetryPolicyConfig retryPolicyConfig,
        TransactionMode transactionMode = TransactionMode.SerializableRw,
        CancellationToken cancellationToken = default
    ) => GetExecutor(retryPolicyConfig).ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        await using var transaction = ydbConnection.BeginTransaction(transactionMode);
        var result = await func(ydbConnection, ct);
        await transaction.CommitAsync(ct);
        return result;
    }, cancellationToken);

    public Task ExecuteInTransactionAsync(
        Func<YdbConnection, CancellationToken, Task> func,
        IRetryPolicy retryPolicy,
        TransactionMode transactionMode = TransactionMode.SerializableRw,
        CancellationToken cancellationToken = default
    ) => new YdbRetryPolicyExecutor(retryPolicy).ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        await using var transaction = ydbConnection.BeginTransaction(transactionMode);
        await func(ydbConnection, ct);
        await transaction.CommitAsync(ct);
    }, cancellationToken);

    public Task<TResult> ExecuteInTransactionAsync<TResult>(
        Func<YdbConnection, CancellationToken, Task<TResult>> func,
        IRetryPolicy retryPolicy,
        TransactionMode transactionMode = TransactionMode.SerializableRw,
        CancellationToken cancellationToken = default
    ) => new YdbRetryPolicyExecutor(retryPolicy).ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        await using var transaction = ydbConnection.BeginTransaction(transactionMode);
        var result = await func(ydbConnection, ct);
        await transaction.CommitAsync(ct);
        return result;
    }, cancellationToken);
}
