using System.Collections.Concurrent;
using Ydb.Sdk.Ado.RetryPolicy;
#if NET7_0_OR_GREATER
using System.Data.Common;
#endif

namespace Ydb.Sdk.Ado;

/// <summary>
/// Represents a data source for YDB connections with built-in retry policy support.
/// </summary>
/// <remarks>
/// YdbDataSource provides a modern, lightweight way to work with YDB databases.
/// It automatically manages connection lifecycle and provides built-in retry policy support
/// for handling transient failures. The data source can execute operations with automatic
/// retry logic and transaction management.
/// 
/// For more information about YDB, see:
/// <see href="https://ydb.tech/docs">YDB Documentation</see>.
/// </remarks>
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

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbDataSource"/> class with default settings.
    /// </summary>
    /// <remarks>
    /// Creates a new data source with default connection string and retry policy settings.
    /// </remarks>
    public YdbDataSource() : this(new YdbDataSourceBuilder())
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbDataSource"/> class with the specified connection string.
    /// </summary>
    /// <param name="connectionString">The connection string to use for database connections.</param>
    /// <remarks>
    /// Creates a new data source with the specified connection string and default retry policy.
    /// </remarks>
    public YdbDataSource(string connectionString) : this(new YdbDataSourceBuilder(connectionString))
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="YdbDataSource"/> class with the specified connection string builder.
    /// </summary>
    /// <param name="connectionStringBuilder">The connection string builder to use for database connections.</param>
    /// <remarks>
    /// Creates a new data source with the specified connection string builder and default retry policy.
    /// </remarks>
    public YdbDataSource(YdbConnectionStringBuilder connectionStringBuilder)
        : this(new YdbDataSourceBuilder(connectionStringBuilder))
    {
    }

    internal YdbDataSource(YdbDataSourceBuilder builder)
    {
        _ydbConnectionStringBuilder = builder.ConnectionStringBuilder;
        _retryPolicyExecutor = new YdbRetryPolicyExecutor(builder.RetryPolicy);
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

    /// <summary>
    /// Creates a new YDB connection.
    /// </summary>
    /// <returns>A new <see cref="YdbConnection"/> instance.</returns>
    /// <remarks>
    /// Creates a new connection that must be opened before use.
    /// The connection should be disposed when no longer needed.
    /// </remarks>
    public
#if NET7_0_OR_GREATER
        new
#endif
        YdbConnection CreateConnection() => CreateDbConnection();

    /// <summary>
    /// Creates and opens a new YDB connection.
    /// </summary>
    /// <returns>A new opened <see cref="YdbConnection"/> instance.</returns>
    /// <exception cref="YdbException">Thrown when the connection cannot be opened.</exception>
    /// <remarks>
    /// Creates a new connection and opens it immediately.
    /// The connection should be disposed when no longer needed.
    /// </remarks>
    public
#if NET7_0_OR_GREATER
        new
#endif
        YdbConnection OpenConnection() => OpenDbConnection();

    /// <summary>
    /// Asynchronously creates and opens a new YDB connection.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation that returns an opened <see cref="YdbConnection"/>.</returns>
    /// <exception cref="YdbException">Thrown when the connection cannot be opened.</exception>
    /// <remarks>
    /// Creates a new connection and opens it asynchronously.
    /// The connection should be disposed when no longer needed.
    /// </remarks>
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

    /// <summary>
    /// Executes an operation with automatic retry policy support.
    /// </summary>
    /// <param name="func">The operation to execute with a YDB connection.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Executes the provided function with automatic retry logic for transient failures.
    /// The connection is automatically managed and disposed after the operation.
    /// </remarks>
    public Task ExecuteAsync(Func<YdbConnection, Task> func) => _retryPolicyExecutor
        .ExecuteAsync(async cancellationToken =>
        {
            await using var ydbConnection = await OpenConnectionAsync(cancellationToken);
            await func(ydbConnection);
        });

    /// <summary>
    /// Executes an operation with automatic retry policy support and returns a result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned by the operation.</typeparam>
    /// <param name="func">The operation to execute with a YDB connection.</param>
    /// <returns>A task representing the asynchronous operation that returns the result.</returns>
    /// <remarks>
    /// Executes the provided function with automatic retry logic for transient failures.
    /// The connection is automatically managed and disposed after the operation.
    /// </remarks>
    public Task<TResult> ExecuteAsync<TResult>(Func<YdbConnection, Task<TResult>> func) => _retryPolicyExecutor
        .ExecuteAsync<TResult>(async cancellationToken =>
        {
            await using var ydbConnection = await OpenConnectionAsync(cancellationToken);
            return await func(ydbConnection);
        });

    /// <summary>
    /// Executes an operation with automatic retry policy support and cancellation token support.
    /// </summary>
    /// <param name="func">The operation to execute with a YDB connection and cancellation token.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Executes the provided function with automatic retry logic for transient failures.
    /// The connection is automatically managed and disposed after the operation.
    /// </remarks>
    public Task ExecuteAsync(
        Func<YdbConnection, CancellationToken, Task> func,
        CancellationToken cancellationToken = default
    ) => _retryPolicyExecutor.ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        await func(ydbConnection, ct);
    }, cancellationToken);

    /// <summary>
    /// Executes an operation with automatic retry policy support, cancellation token support, and returns a result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned by the operation.</typeparam>
    /// <param name="func">The operation to execute with a YDB connection and cancellation token.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation that returns the result.</returns>
    /// <remarks>
    /// Executes the provided function with automatic retry logic for transient failures.
    /// The connection is automatically managed and disposed after the operation.
    /// </remarks>
    public Task<TResult> ExecuteAsync<TResult>(
        Func<YdbConnection, CancellationToken, Task<TResult>> func,
        CancellationToken cancellationToken = default
    ) => _retryPolicyExecutor.ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        return await func(ydbConnection, ct);
    }, cancellationToken);

    /// <summary>
    /// Executes an operation with a custom retry policy configuration.
    /// </summary>
    /// <param name="func">The operation to execute with a YDB connection.</param>
    /// <param name="retryPolicyConfig">The retry policy configuration to use for this operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Executes the provided function with the specified retry policy configuration.
    /// The connection is automatically managed and disposed after the operation.
    /// </remarks>
    public Task ExecuteAsync(
        Func<YdbConnection, Task> func,
        YdbRetryPolicyConfig retryPolicyConfig
    ) => GetExecutor(retryPolicyConfig).ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        await func(ydbConnection);
    });

    /// <summary>
    /// Executes an operation with a custom retry policy.
    /// </summary>
    /// <param name="func">The operation to execute with a YDB connection.</param>
    /// <param name="retryPolicy">The custom retry policy to use for this operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Executes the provided function with the specified custom retry policy.
    /// The connection is automatically managed and disposed after the operation.
    /// </remarks>
    public Task ExecuteAsync(
        Func<YdbConnection, Task> func,
        IRetryPolicy retryPolicy
    ) => new YdbRetryPolicyExecutor(retryPolicy).ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        await func(ydbConnection);
    });

    /// <summary>
    /// Executes an operation with a custom retry policy configuration and returns a result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned by the operation.</typeparam>
    /// <param name="func">The operation to execute with a YDB connection.</param>
    /// <param name="retryPolicyConfig">The retry policy configuration to use for this operation.</param>
    /// <returns>A task representing the asynchronous operation that returns the result.</returns>
    /// <remarks>
    /// Executes the provided function with the specified retry policy configuration.
    /// The connection is automatically managed and disposed after the operation.
    /// </remarks>
    public Task<TResult> ExecuteAsync<TResult>(
        Func<YdbConnection, Task<TResult>> func,
        YdbRetryPolicyConfig retryPolicyConfig
    ) => GetExecutor(retryPolicyConfig).ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        return await func(ydbConnection);
    });

    /// <summary>
    /// Executes an operation with a custom retry policy and returns a result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned by the operation.</typeparam>
    /// <param name="func">The operation to execute with a YDB connection.</param>
    /// <param name="retryPolicy">The custom retry policy to use for this operation.</param>
    /// <returns>A task representing the asynchronous operation that returns the result.</returns>
    /// <remarks>
    /// Executes the provided function with the specified custom retry policy.
    /// The connection is automatically managed and disposed after the operation.
    /// </remarks>
    public Task<TResult> ExecuteAsync<TResult>(
        Func<YdbConnection, Task<TResult>> func,
        IRetryPolicy retryPolicy
    ) => new YdbRetryPolicyExecutor(retryPolicy).ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        return await func(ydbConnection);
    });

    /// <summary>
    /// Executes an operation with a custom retry policy configuration and cancellation token support.
    /// </summary>
    /// <param name="func">The operation to execute with a YDB connection and cancellation token.</param>
    /// <param name="retryPolicyConfig">The retry policy configuration to use for this operation.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Executes the provided function with the specified retry policy configuration and cancellation support.
    /// The connection is automatically managed and disposed after the operation.
    /// </remarks>
    public Task ExecuteAsync(
        Func<YdbConnection, CancellationToken, Task> func,
        YdbRetryPolicyConfig retryPolicyConfig,
        CancellationToken cancellationToken = default
    ) => GetExecutor(retryPolicyConfig).ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        await func(ydbConnection, ct);
    }, cancellationToken);

    /// <summary>
    /// Executes an operation with a custom retry policy and cancellation token support.
    /// </summary>
    /// <param name="func">The operation to execute with a YDB connection and cancellation token.</param>
    /// <param name="retryPolicy">The custom retry policy to use for this operation.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Executes the provided function with the specified custom retry policy and cancellation support.
    /// The connection is automatically managed and disposed after the operation.
    /// </remarks>
    public Task ExecuteAsync(
        Func<YdbConnection, CancellationToken, Task> func,
        IRetryPolicy retryPolicy,
        CancellationToken cancellationToken = default
    ) => new YdbRetryPolicyExecutor(retryPolicy).ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        await func(ydbConnection, ct);
    }, cancellationToken);

    /// <summary>
    /// Executes an operation with a custom retry policy configuration, cancellation token support, and returns a result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned by the operation.</typeparam>
    /// <param name="func">The operation to execute with a YDB connection and cancellation token.</param>
    /// <param name="retryPolicyConfig">The retry policy configuration to use for this operation.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation that returns the result.</returns>
    /// <remarks>
    /// Executes the provided function with the specified retry policy configuration and cancellation support.
    /// The connection is automatically managed and disposed after the operation.
    /// </remarks>
    public Task<TResult> ExecuteAsync<TResult>(
        Func<YdbConnection, CancellationToken, Task<TResult>> func,
        YdbRetryPolicyConfig retryPolicyConfig,
        CancellationToken cancellationToken = default
    ) => GetExecutor(retryPolicyConfig).ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        return await func(ydbConnection, ct);
    }, cancellationToken);

    /// <summary>
    /// Executes an operation with a custom retry policy, cancellation token support, and returns a result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned by the operation.</typeparam>
    /// <param name="func">The operation to execute with a YDB connection and cancellation token.</param>
    /// <param name="retryPolicy">The custom retry policy to use for this operation.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation that returns the result.</returns>
    /// <remarks>
    /// Executes the provided function with the specified custom retry policy and cancellation support.
    /// The connection is automatically managed and disposed after the operation.
    /// </remarks>
    public Task<TResult> ExecuteAsync<TResult>(
        Func<YdbConnection, CancellationToken, Task<TResult>> func,
        IRetryPolicy retryPolicy,
        CancellationToken cancellationToken = default
    ) => new YdbRetryPolicyExecutor(retryPolicy).ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        return await func(ydbConnection, ct);
    }, cancellationToken);

    /// <summary>
    /// Executes an operation within a transaction with automatic retry policy support.
    /// </summary>
    /// <param name="func">The operation to execute within the transaction.</param>
    /// <param name="transactionMode">The transaction mode to use. Default is SerializableRw.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Executes the provided function within a transaction with automatic retry logic.
    /// The transaction is automatically committed on success or rolled back on failure.
    /// The connection and transaction are automatically managed and disposed.
    /// </remarks>
    public Task ExecuteInTransactionAsync(
        Func<YdbConnection, Task> func,
        TransactionMode transactionMode = TransactionMode.SerializableRw
    ) => _retryPolicyExecutor.ExecuteAsync(async cancellationToken =>
    {
        await using var ydbConnection = await OpenConnectionAsync(cancellationToken);
        await using var transaction = ydbConnection.BeginTransaction(transactionMode);
        await func(ydbConnection);
        await transaction.CommitAsync(cancellationToken);
    });

    /// <summary>
    /// Executes an operation within a transaction with automatic retry policy support and returns a result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned by the operation.</typeparam>
    /// <param name="func">The operation to execute within the transaction.</param>
    /// <param name="transactionMode">The transaction mode to use. Default is SerializableRw.</param>
    /// <returns>A task representing the asynchronous operation that returns the result.</returns>
    /// <remarks>
    /// Executes the provided function within a transaction with automatic retry logic.
    /// The transaction is automatically committed on success or rolled back on failure.
    /// The connection and transaction are automatically managed and disposed.
    /// </remarks>
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

    /// <summary>
    /// Executes an operation within a transaction with automatic retry policy support and cancellation token support.
    /// </summary>
    /// <param name="func">The operation to execute within the transaction with cancellation token support.</param>
    /// <param name="transactionMode">The transaction mode to use. Default is SerializableRw.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Executes the provided function within a transaction with automatic retry logic and cancellation support.
    /// The transaction is automatically committed on success or rolled back on failure.
    /// The connection and transaction are automatically managed and disposed.
    /// </remarks>
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

    /// <summary>
    /// Executes an operation within a transaction with automatic retry policy support, cancellation token support, and returns a result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned by the operation.</typeparam>
    /// <param name="func">The operation to execute within the transaction with cancellation token support.</param>
    /// <param name="transactionMode">The transaction mode to use. Default is SerializableRw.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation that returns the result.</returns>
    /// <remarks>
    /// Executes the provided function within a transaction with automatic retry logic and cancellation support.
    /// The transaction is automatically committed on success or rolled back on failure.
    /// The connection and transaction are automatically managed and disposed.
    /// </remarks>
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

    /// <summary>
    /// Executes an operation within a transaction with a custom retry policy configuration.
    /// </summary>
    /// <param name="func">The operation to execute within the transaction.</param>
    /// <param name="retryPolicyConfig">The retry policy configuration to use for this operation.</param>
    /// <param name="transactionMode">The transaction mode to use. Default is SerializableRw.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Executes the provided function within a transaction with the specified retry policy configuration.
    /// The transaction is automatically committed on success or rolled back on failure.
    /// The connection and transaction are automatically managed and disposed.
    /// </remarks>
    public Task ExecuteInTransactionAsync(
        Func<YdbConnection, Task> func,
        YdbRetryPolicyConfig retryPolicyConfig,
        TransactionMode transactionMode = TransactionMode.SerializableRw
    ) => GetExecutor(retryPolicyConfig).ExecuteAsync(async cancellationToken =>
    {
        await using var ydbConnection = await OpenConnectionAsync(cancellationToken);
        await using var transaction = ydbConnection.BeginTransaction(transactionMode);
        await func(ydbConnection);
        await transaction.CommitAsync(cancellationToken);
    });

    /// <summary>
    /// Executes an operation within a transaction with a custom retry policy configuration and returns a result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned by the operation.</typeparam>
    /// <param name="func">The operation to execute within the transaction.</param>
    /// <param name="retryPolicyConfig">The retry policy configuration to use for this operation.</param>
    /// <param name="transactionMode">The transaction mode to use. Default is SerializableRw.</param>
    /// <returns>A task representing the asynchronous operation that returns the result.</returns>
    /// <remarks>
    /// Executes the provided function within a transaction with the specified retry policy configuration.
    /// The transaction is automatically committed on success or rolled back on failure.
    /// The connection and transaction are automatically managed and disposed.
    /// </remarks>
    public Task<TResult> ExecuteInTransactionAsync<TResult>(
        Func<YdbConnection, Task<TResult>> func,
        YdbRetryPolicyConfig retryPolicyConfig,
        TransactionMode transactionMode = TransactionMode.SerializableRw
    ) => GetExecutor(retryPolicyConfig).ExecuteAsync(async cancellationToken =>
    {
        await using var ydbConnection = await OpenConnectionAsync(cancellationToken);
        await using var transaction = ydbConnection.BeginTransaction(transactionMode);
        var result = await func(ydbConnection);
        await transaction.CommitAsync(cancellationToken);
        return result;
    });

    /// <summary>
    /// Executes an operation within a transaction with a custom retry policy configuration and cancellation token support.
    /// </summary>
    /// <param name="func">The operation to execute within the transaction with cancellation token support.</param>
    /// <param name="retryPolicyConfig">The retry policy configuration to use for this operation.</param>
    /// <param name="transactionMode">The transaction mode to use. Default is SerializableRw.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Executes the provided function within a transaction with the specified retry policy configuration and cancellation support.
    /// The transaction is automatically committed on success or rolled back on failure.
    /// The connection and transaction are automatically managed and disposed.
    /// </remarks>
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

    /// <summary>
    /// Executes an operation within a transaction with a custom retry policy configuration, cancellation token support, and returns a result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned by the operation.</typeparam>
    /// <param name="func">The operation to execute within the transaction with cancellation token support.</param>
    /// <param name="retryPolicyConfig">The retry policy configuration to use for this operation.</param>
    /// <param name="transactionMode">The transaction mode to use. Default is SerializableRw.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation that returns the result.</returns>
    /// <remarks>
    /// Executes the provided function within a transaction with the specified retry policy configuration and cancellation support.
    /// The transaction is automatically committed on success or rolled back on failure.
    /// The connection and transaction are automatically managed and disposed.
    /// </remarks>
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

    /// <summary>
    /// Executes an operation within a transaction with a custom retry policy.
    /// </summary>
    /// <param name="func">The operation to execute within the transaction.</param>
    /// <param name="retryPolicy">The custom retry policy to use for this operation.</param>
    /// <param name="transactionMode">The transaction mode to use. Default is SerializableRw.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Executes the provided function within a transaction with the specified custom retry policy.
    /// The transaction is automatically committed on success or rolled back on failure.
    /// The connection and transaction are automatically managed and disposed.
    /// </remarks>
    public Task ExecuteInTransactionAsync(
        Func<YdbConnection, Task> func,
        IRetryPolicy retryPolicy,
        TransactionMode transactionMode = TransactionMode.SerializableRw
    ) => new YdbRetryPolicyExecutor(retryPolicy).ExecuteAsync(async ct =>
    {
        await using var ydbConnection = await OpenConnectionAsync(ct);
        await using var transaction = ydbConnection.BeginTransaction(transactionMode);
        await func(ydbConnection);
        await transaction.CommitAsync(ct);
    });

    /// <summary>
    /// Executes an operation within a transaction with a custom retry policy and returns a result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned by the operation.</typeparam>
    /// <param name="func">The operation to execute within the transaction.</param>
    /// <param name="retryPolicy">The custom retry policy to use for this operation.</param>
    /// <param name="transactionMode">The transaction mode to use. Default is SerializableRw.</param>
    /// <returns>A task representing the asynchronous operation that returns the result.</returns>
    /// <remarks>
    /// Executes the provided function within a transaction with the specified custom retry policy.
    /// The transaction is automatically committed on success or rolled back on failure.
    /// The connection and transaction are automatically managed and disposed.
    /// </remarks>
    public Task<TResult> ExecuteInTransactionAsync<TResult>(
        Func<YdbConnection, Task<TResult>> func,
        IRetryPolicy retryPolicy,
        TransactionMode transactionMode = TransactionMode.SerializableRw
    ) => new YdbRetryPolicyExecutor(retryPolicy).ExecuteAsync(async cancellationToken =>
    {
        await using var ydbConnection = await OpenConnectionAsync(cancellationToken);
        await using var transaction = ydbConnection.BeginTransaction(transactionMode);
        var result = await func(ydbConnection);
        await transaction.CommitAsync(cancellationToken);
        return result;
    });

    /// <summary>
    /// Executes an operation within a transaction with a custom retry policy and cancellation token support.
    /// </summary>
    /// <param name="func">The operation to execute within the transaction with cancellation token support.</param>
    /// <param name="retryPolicy">The custom retry policy to use for this operation.</param>
    /// <param name="transactionMode">The transaction mode to use. Default is SerializableRw.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Executes the provided function within a transaction with the specified custom retry policy and cancellation support.
    /// The transaction is automatically committed on success or rolled back on failure.
    /// The connection and transaction are automatically managed and disposed.
    /// </remarks>
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

    /// <summary>
    /// Executes an operation within a transaction with a custom retry policy, cancellation token support, and returns a result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned by the operation.</typeparam>
    /// <param name="func">The operation to execute within the transaction with cancellation token support.</param>
    /// <param name="retryPolicy">The custom retry policy to use for this operation.</param>
    /// <param name="transactionMode">The transaction mode to use. Default is SerializableRw.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation that returns the result.</returns>
    /// <remarks>
    /// Executes the provided function within a transaction with the specified custom retry policy and cancellation support.
    /// The transaction is automatically committed on success or rolled back on failure.
    /// The connection and transaction are automatically managed and disposed.
    /// </remarks>
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

    /// <summary>
    /// Asynchronously creates and opens a new YDB connection with retry policy support.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation that returns an opened <see cref="YdbConnection"/>.</returns>
    /// <remarks>
    /// Creates a new connection and opens it asynchronously with automatic retry logic for transient failures.
    /// The connection should be disposed when no longer needed.
    /// 
    /// 
    /// <strong>Important limitations:</strong>
    /// - Retryable connections do not support interactive transactions. Use <see cref="ExecuteInTransactionAsync"/> methods instead.
    /// - On large result sets, these connections may cause OutOfMemoryException as they read all data into memory. Use with caution.
    /// </remarks>
    public async ValueTask<YdbConnection> OpenRetryableConnectionAsync(CancellationToken cancellationToken = default)
    {
        var ydbConnection = CreateDbConnection();
        try
        {
            await ydbConnection.OpenAsync(_retryPolicyExecutor, cancellationToken);

            return ydbConnection;
        }
        catch
        {
            await ydbConnection.CloseAsync();
            throw;
        }
    }

    /// <summary>
    /// Asynchronously creates and opens a new YDB connection with a custom retry policy configuration.
    /// </summary>
    /// <param name="ydbRetryPolicyConfig">The retry policy configuration to use for opening the connection.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation that returns an opened <see cref="YdbConnection"/>.</returns>
    /// <remarks>
    /// Creates a new connection and opens it asynchronously with the specified retry policy configuration.
    /// The connection should be disposed when no longer needed.
    /// 
    /// 
    /// <strong>Important limitations:</strong>
    /// - Retryable connections do not support interactive transactions. Use <see cref="ExecuteInTransactionAsync"/> methods instead.
    /// - On large result sets, these connections may cause OutOfMemoryException as they read all data into memory. Use with caution.
    /// </remarks>
    public async ValueTask<YdbConnection> OpenRetryableConnectionAsync(
        YdbRetryPolicyConfig ydbRetryPolicyConfig,
        CancellationToken cancellationToken = default
    )
    {
        var ydbConnection = CreateDbConnection();
        try
        {
            await ydbConnection.OpenAsync(GetExecutor(ydbRetryPolicyConfig), cancellationToken);

            return ydbConnection;
        }
        catch
        {
            await ydbConnection.CloseAsync();
            throw;
        }
    }

    /// <summary>
    /// Asynchronously creates and opens a new YDB connection with a custom retry policy.
    /// </summary>
    /// <param name="retryPolicy">The custom retry policy to use for opening the connection.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation that returns an opened <see cref="YdbConnection"/>.</returns>
    /// <remarks>
    /// Creates a new connection and opens it asynchronously with the specified custom retry policy.
    /// The connection should be disposed when no longer needed.
    /// 
    /// <para>Important limitations:</para>
    /// <para>
    /// - Retryable connections do not support interactive transactions.
    /// Use ExecuteInTransactionAsync methods instead.
    /// </para>
    /// <para>
    /// - On large result sets, these connections may cause OutOfMemoryException as they read all data into memory.
    /// Use with caution.
    /// </para>
    /// </remarks>
    public async ValueTask<YdbConnection> OpenRetryableConnectionAsync(
        IRetryPolicy retryPolicy,
        CancellationToken cancellationToken = default
    )
    {
        var ydbConnection = CreateDbConnection();
        try
        {
            await ydbConnection.OpenAsync(new YdbRetryPolicyExecutor(retryPolicy), cancellationToken);

            return ydbConnection;
        }
        catch
        {
            await ydbConnection.CloseAsync();
            throw;
        }
    }
}
