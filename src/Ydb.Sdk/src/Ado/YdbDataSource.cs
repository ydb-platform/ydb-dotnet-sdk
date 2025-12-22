using System.Collections.Concurrent;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Ado.RetryPolicy;
using Ydb.Sdk.Ado.Schema;
using Ydb.Sdk.Ado.Session;
using Ydb.Table;
using Ydb.Table.V1;
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
/// <para>
/// For more information about YDB, see:
/// <see href="https://ydb.tech/docs">YDB Documentation</see>.
/// </para>
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

    private ISessionSource? _sessionSource;

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
    /// Creates a new <see cref="YdbConnection"/>.
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
    /// Creates and opens a new <see cref="YdbConnection"/>.
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
    /// Asynchronously creates and opens a new <see cref="YdbConnection"/>.
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
    /// <param name="func">The operation to execute with a <see cref="YdbConnection"/>.</param>
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
    /// Executes an operation with retry policy support and returns a result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned by the operation.</typeparam>
    /// <param name="func">The operation to execute with a <see cref="YdbConnection"/>.</param>
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
    /// Executes an operation with retry policy and cancellation token support.
    /// </summary>
    /// <param name="func">The operation to execute with a <see cref="YdbConnection"/> and cancellation token.</param>
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
    /// Executes an operation with retry policy and cancellation token support and returns a result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned by the operation.</typeparam>
    /// <param name="func">The operation to execute with a <see cref="YdbConnection"/> and cancellation token.</param>
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
    /// Executes an operation with a custom <see cref="YdbRetryPolicyConfig"/>.
    /// </summary>
    /// <param name="func">The operation to execute with a <see cref="YdbConnection"/>.</param>
    /// <param name="retryPolicyConfig">The <see cref="YdbRetryPolicyConfig"/> to use for this operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Executes the provided function with the specified <see cref="YdbRetryPolicyConfig"/>.
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
    /// <param name="func">The operation to execute with a <see cref="YdbConnection"/>.</param>
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
    /// Executes an operation with a custom <see cref="YdbRetryPolicyConfig"/> and returns a result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned by the operation.</typeparam>
    /// <param name="func">The operation to execute with a <see cref="YdbConnection"/>.</param>
    /// <param name="retryPolicyConfig">The <see cref="YdbRetryPolicyConfig"/> to use for this operation.</param>
    /// <returns>A task representing the asynchronous operation that returns the result.</returns>
    /// <remarks>
    /// Executes the provided function with the specified <see cref="YdbRetryPolicyConfig"/>.
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
    /// <param name="func">The operation to execute with a <see cref="YdbConnection"/>.</param>
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
    /// Executes an operation with a custom <see cref="YdbRetryPolicyConfig"/> and cancellation token support.
    /// </summary>
    /// <param name="func">The operation to execute with a <see cref="YdbConnection"/> and cancellation token.</param>
    /// <param name="retryPolicyConfig">The <see cref="YdbRetryPolicyConfig"/> to use for this operation.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Executes the provided function with the specified <see cref="YdbRetryPolicyConfig"/> and cancellation support.
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
    /// <param name="func">The operation to execute with a <see cref="YdbConnection"/> and cancellation token.</param>
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
    /// Executes an operation with a custom <see cref="YdbRetryPolicyConfig"/>, cancellation token support and returns a result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned by the operation.</typeparam>
    /// <param name="func">The operation to execute with a <see cref="YdbConnection"/> and cancellation token.</param>
    /// <param name="retryPolicyConfig">The <see cref="YdbRetryPolicyConfig"/> to use for this operation.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation that returns the result.</returns>
    /// <remarks>
    /// Executes the provided function with the specified <see cref="YdbRetryPolicyConfig"/> and cancellation support.
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
    /// Executes an operation with a custom retry policy, cancellation token support and returns a result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned by the operation.</typeparam>
    /// <param name="func">The operation to execute with a <see cref="YdbConnection"/> and cancellation token.</param>
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
    /// Executes an operation within a transaction with retry policy and cancellation token support and returns a result.
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
    /// Executes an operation within a transaction with a custom <see cref="YdbRetryPolicyConfig"/>.
    /// </summary>
    /// <param name="func">The operation to execute within the transaction.</param>
    /// <param name="retryPolicyConfig">The <see cref="YdbRetryPolicyConfig"/> to use for this operation.</param>
    /// <param name="transactionMode">The transaction mode to use. Default is SerializableRw.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Executes the provided function within a transaction with the specified <see cref="YdbRetryPolicyConfig"/>.
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
    /// Executes an operation within a transaction with a custom <see cref="YdbRetryPolicyConfig"/> and returns a result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned by the operation.</typeparam>
    /// <param name="func">The operation to execute within the transaction.</param>
    /// <param name="retryPolicyConfig">The <see cref="YdbRetryPolicyConfig"/> to use for this operation.</param>
    /// <param name="transactionMode">The transaction mode to use. Default is SerializableRw.</param>
    /// <returns>A task representing the asynchronous operation that returns the result.</returns>
    /// <remarks>
    /// Executes the provided function within a transaction with the specified <see cref="YdbRetryPolicyConfig"/>.
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
    /// Executes an operation within a transaction with a custom <see cref="YdbRetryPolicyConfig"/> and cancellation token support.
    /// </summary>
    /// <param name="func">The operation to execute within the transaction with cancellation token support.</param>
    /// <param name="retryPolicyConfig">The <see cref="YdbRetryPolicyConfig"/> to use for this operation.</param>
    /// <param name="transactionMode">The transaction mode to use. Default is SerializableRw.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <remarks>
    /// Executes the provided function within a transaction with the specified <see cref="YdbRetryPolicyConfig"/> and cancellation support.
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
    /// Executes an operation within a transaction with a custom <see cref="YdbRetryPolicyConfig"/>, cancellation token support and returns a result.
    /// </summary>
    /// <typeparam name="TResult">The type of the result returned by the operation.</typeparam>
    /// <param name="func">The operation to execute within the transaction with cancellation token support.</param>
    /// <param name="retryPolicyConfig">The <see cref="YdbRetryPolicyConfig"/> to use for this operation.</param>
    /// <param name="transactionMode">The transaction mode to use. Default is SerializableRw.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation that returns the result.</returns>
    /// <remarks>
    /// Executes the provided function within a transaction with the specified <see cref="YdbRetryPolicyConfig"/> and cancellation support.
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
    /// Executes an operation within a transaction with a custom retry policy, cancellation token support and returns a result.
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
    /// Asynchronously creates and opens a new <see cref="YdbConnection"/> with retry policy support.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation that returns an opened <see cref="YdbConnection"/>.</returns>
    /// <remarks>
    /// Creates a new connection and opens it asynchronously with automatic retry logic for transient failures.
    /// The connection should be disposed when no longer needed.
    /// 
    /// 
    /// <para>Important limitations:</para>
    /// <para>
    /// - Retryable connections do not support interactive transactions. Use ExecuteInTransactionAsync methods instead.
    /// </para>
    /// <para>
    /// - On large result sets, these connections may cause <see cref="OutOfMemoryException"/> as they read all data into memory. Use with caution.
    /// </para>
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
    /// Asynchronously creates and opens a new <see cref="YdbConnection"/> with a custom <see cref="YdbRetryPolicyConfig"/>.
    /// </summary>
    /// <param name="ydbRetryPolicyConfig">The <see cref="YdbRetryPolicyConfig"/> to use for opening the connection.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation that returns an opened <see cref="YdbConnection"/>.</returns>
    /// <remarks>
    /// Creates a new connection and opens it asynchronously with the specified <see cref="YdbRetryPolicyConfig"/>.
    /// The connection should be disposed when no longer needed.
    /// 
    /// 
    /// <para>Important limitations:</para>
    /// <para>
    /// - Retryable connections do not support interactive transactions. Use ExecuteInTransactionAsync methods instead.
    /// </para>
    /// <para>
    /// - On large result sets, these connections may cause <see cref="OutOfMemoryException"/> as they read all data into memory. Use with caution.
    /// </para>
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
    /// Asynchronously creates and opens a new <see cref="YdbConnection"/> with a custom retry policy.
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

    /// <summary>
    /// Describes a table structure and returns its metadata.
    /// </summary>
    /// <param name="tableName">The name of the table to describe. Can be a simple table name or a full path.</param>
    /// <param name="settings">Optional settings to control what information is included in the response.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation that returns the table description.</returns>
    /// <exception cref="YdbException">Thrown when the table does not exist or the operation fails.</exception>
    /// <remarks>
    /// This method performs a control plane operation to retrieve table metadata including columns, primary key, indexes, and optional statistics.
    /// The operation uses the default retry policy configured for this data source.
    /// 
    /// <para>
    /// The table name can be specified as:
    /// - A simple name (e.g., "users") - will be resolved relative to the database path
    /// - A full path (e.g., "/database/users") - will be used as-is
    /// </para>
    /// </remarks>
    public async Task<YdbTableDescription> DescribeTable(
        string tableName,
        DescribeTableSettings settings = default,
        CancellationToken cancellationToken = default
    ) => await YdbSchema.DescribeTable(await Driver(cancellationToken), tableName, settings, cancellationToken);

    /// <summary>
    /// Copies a table to a new location.
    /// </summary>
    /// <param name="sourceTable">The name or path of the source table to copy.</param>
    /// <param name="destinationTable">The name or path of the destination table where the copy will be created.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="YdbException">Thrown when the source table does not exist, destination already exists, or the operation fails.</exception>
    /// <remarks>
    /// This method performs a control plane operation to copy a table structure and data to a new location.
    /// The operation uses the default retry policy configured for this data source.
    /// 
    /// <para>
    /// Note: This operation copies both the table structure and its data. Indexes are copied by default.
    /// </para>
    /// </remarks>
    public async Task CopyTable(
        string sourceTable,
        string destinationTable,
        CancellationToken cancellationToken = default
    )
    {
        var driver = await Driver(cancellationToken);

        var copyTableResponse = await driver.UnaryCall(TableService.CopyTableMethod, new CopyTableRequest
        {
            SourcePath = FullPath(sourceTable),
            DestinationPath = FullPath(destinationTable)
        }, new GrpcRequestSettings { CancellationToken = cancellationToken });

        if (copyTableResponse.Operation.Status.IsNotSuccess())
            throw YdbException.FromServer(copyTableResponse.Operation);
    }

    /// <summary>
    /// Copies multiple tables to new locations in a single operation.
    /// </summary>
    /// <param name="copyTableSettingsList">A list of copy table settings specifying source and destination tables for each copy operation.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="YdbException">Thrown when any source table does not exist, any destination already exists, or the operation fails.</exception>
    /// <remarks>
    /// This method performs a control plane operation to copy multiple tables in a single transaction.
    /// All copies succeed or fail together. The operation uses the default retry policy configured for this data source.
    /// 
    /// <para>
    /// Each <see cref="CopyTableSettings"/> specifies:
    /// - SourceTable: The name or path of the source table
    /// - DestinationTable: The name or path of the destination table
    /// - OmitIndexes: Whether to skip copying indexes (default: false)
    /// </para>
    /// </remarks>
    public async Task CopyTables(
        IReadOnlyList<CopyTableSettings> copyTableSettingsList,
        CancellationToken cancellationToken = default
    )
    {
        var driver = await Driver(cancellationToken);
        var copyTablesRequest = new CopyTablesRequest();
        foreach (var copyTable in copyTableSettingsList)
            copyTablesRequest.Tables.Add(new CopyTableItem
            {
                SourcePath = FullPath(copyTable.SourceTable),
                DestinationPath = FullPath(copyTable.DestinationTable),
                OmitIndexes = copyTable.OmitIndexes
            });

        var copyTablesResponse = await driver.UnaryCall(TableService.CopyTablesMethod, copyTablesRequest,
            new GrpcRequestSettings { CancellationToken = cancellationToken });

        if (copyTablesResponse.Operation.Status.IsNotSuccess())
            throw YdbException.FromServer(copyTablesResponse.Operation);
    }

    /// <summary>
    /// Renames multiple tables in a single operation.
    /// </summary>
    /// <param name="renameTableSettingsList">A list of rename table settings specifying source and destination names for each rename operation.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="YdbException">Thrown when any source table does not exist, any destination already exists (unless ReplaceDestination is true), or the operation fails.</exception>
    /// <remarks>
    /// This method performs a control plane operation to rename multiple tables in a single transaction.
    /// All renames succeed or fail together. The operation uses the default retry policy configured for this data source.
    /// 
    /// <para>
    /// Each <see cref="RenameTableSettings"/> specifies:
    /// - SourceTable: The current name or path of the table
    /// - DestinationTable: The new name or path for the table
    /// - ReplaceDestination: Whether to replace the destination if it already exists (default: false)
    /// </para>
    /// </remarks>
    public async Task RenameTables(
        IReadOnlyList<RenameTableSettings> renameTableSettingsList,
        CancellationToken cancellationToken = default
    )
    {
        var driver = await Driver(cancellationToken);
        var renameTablesRequest = new RenameTablesRequest();
        foreach (var renameTable in renameTableSettingsList)
            renameTablesRequest.Tables.Add(new RenameTableItem
            {
                SourcePath = FullPath(renameTable.SourceTable),
                DestinationPath = FullPath(renameTable.DestinationTable),
                ReplaceDestination = renameTable.ReplaceDestination
            });

        var renameTablesResponse = await driver.UnaryCall(TableService.RenameTablesMethod, renameTablesRequest,
            new GrpcRequestSettings { CancellationToken = cancellationToken });

        if (renameTablesResponse.Operation.Status.IsNotSuccess())
            throw YdbException.FromServer(renameTablesResponse.Operation);
    }

    /// <summary>
    /// Drops (deletes) a table.
    /// </summary>
    /// <param name="tableName">The name or path of the table to drop.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="YdbException">Thrown when the table does not exist or the operation fails.</exception>
    /// <remarks>
    /// This method performs a control plane operation to permanently delete a table and all its data.
    /// The operation uses the default retry policy configured for this data source.
    /// 
    /// <para>
    /// Warning: This operation cannot be undone. All data in the table will be permanently lost.
    /// </para>
    /// </remarks>
    public async Task DropTable(string tableName, CancellationToken cancellationToken = default)
    {
        var driver = await Driver(cancellationToken);

        var dropTableResponse = await driver.UnaryCall(
            TableService.DropTableMethod,
            new DropTableRequest { Path = FullPath(tableName) },
            new GrpcRequestSettings { CancellationToken = cancellationToken }
        );

        if (dropTableResponse.Operation.Status.IsNotSuccess())
            throw YdbException.FromServer(dropTableResponse.Operation);
    }

    /// <summary>
    /// Creates a new table with the specified structure.
    /// </summary>
    /// <param name="tableDescription">The table description containing columns, primary key, indexes, and other table properties.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    /// <exception cref="YdbException">Thrown when the table already exists or the operation fails.</exception>
    /// <exception cref="NotSupportedException">Thrown when attempting to create an External table type (not supported via Control Plane RPC).</exception>
    /// <remarks>
    /// This method performs a control plane operation to create a new table with the specified schema.
    /// The operation uses the default retry policy configured for this data source.
    /// 
    /// <para>
    /// The <see cref="YdbTableDescription"/> must include:
    /// - Name: The table name or path
    /// - Columns: At least one column definition
    /// - PrimaryKey: At least one primary key column name
    /// 
    /// Optional properties:
    /// - Type: Table storage type (Raw or Column, default: Raw)
    /// - Indexes: List of indexes to create
    /// </para>
    /// 
    /// <para>
    /// Supported table types:
    /// - Raw: Row-oriented storage (default)
    /// - Column: Column-oriented storage
    /// - External: Not supported via Control Plane RPC (use ExecuteSchemeQuery instead)
    /// </para>
    /// </remarks>
    public async Task CreateTable(
        YdbTableDescription tableDescription,
        CancellationToken cancellationToken = default
    )
    {
        var driver = await Driver(cancellationToken);
        var createTableRequest = new CreateTableRequest
        {
            Path = FullPath(tableDescription.Name),
            StoreType = tableDescription.Type switch
            {
                YdbTableType.Raw => StoreType.Row,
                YdbTableType.Column => StoreType.Column,
                YdbTableType.External => throw new NotSupportedException(
                    "`External` isn't supported on the Control Plane RPC."),
                _ => throw new ArgumentOutOfRangeException(nameof(tableDescription.Type), "Unknown table type")
            }
        };

        foreach (var column in tableDescription.Columns)
            createTableRequest.Columns.Add(column.ToProto());

        foreach (var index in tableDescription.Indexes)
            createTableRequest.Indexes.Add(index.ToProto());

        foreach (var pkColumn in tableDescription.PrimaryKey)
            createTableRequest.PrimaryKey.Add(pkColumn);

        var createTableResponse = await driver.UnaryCall(TableService.CreateTableMethod, createTableRequest,
            new GrpcRequestSettings { CancellationToken = cancellationToken });
        
        if (createTableResponse.Operation.Status.IsNotSuccess())
            throw YdbException.FromServer(createTableResponse.Operation);
    }

    private string FullPath(string tableName) => tableName.FullPath(_ydbConnectionStringBuilder.Database);

    private async ValueTask<IDriver> Driver(CancellationToken cancellationToken) =>
        (_sessionSource ??= await PoolManager.Get(_ydbConnectionStringBuilder, cancellationToken)).Driver;
}
