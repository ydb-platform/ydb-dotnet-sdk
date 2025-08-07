#if NET7_0_OR_GREATER
using System.Data.Common;
using Ydb.Sdk.Retry;
using Ydb.Sdk.Retry.Classifier;

namespace Ydb.Sdk.Ado;

public class YdbDataSource : DbDataSource
{
    private readonly YdbConnectionStringBuilder _ydbConnectionStringBuilder;

    private static readonly YdbRetryPolicy DefaultRetryPolicy = new();
    private TimeSpan DefaultTimeout { get; init; } = TimeSpan.FromSeconds(30);
    private int MaxRetryAttempts   { get; init; } = YdbRetryPolicy.DefaultMaxAttempts;

    private YdbDataSource(YdbConnectionStringBuilder connectionStringBuilder)
        : this() => _ydbConnectionStringBuilder = connectionStringBuilder;

    public YdbDataSource(string connectionString)
        : this(new YdbConnectionStringBuilder(connectionString)) { }

    public YdbDataSource()
    {
        _ydbConnectionStringBuilder = new YdbConnectionStringBuilder();
    }

    public YdbDataSource(YdbConnectionStringBuilder csb, TimeSpan defaultTimeout,
        int maxAttempts = YdbRetryPolicy.DefaultMaxAttempts)
        : this(csb)
    {
        DefaultTimeout = defaultTimeout;
        MaxRetryAttempts = maxAttempts;
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
            try
            {
                dbConnection.Close(); 
            }
            catch
            {
                // ignored
            }
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
            try
            {
                await ydbConnection.CloseAsync();
            }
            catch
            {
                // ignored
            }
            throw;
        }
    }

    public override string ConnectionString => _ydbConnectionStringBuilder.ConnectionString;

    protected override async ValueTask DisposeAsyncCore() =>
        await PoolManager.ClearPool(_ydbConnectionStringBuilder.ConnectionString);

    protected override void Dispose(bool disposing)
    {
        try { DisposeAsyncCore().AsTask().GetAwaiter().GetResult(); }
        catch
        {
            // ignored
        }
    }

    public async Task<TResult> ExecuteWithRetryAsync<TResult>(
        Func<CancellationToken, Task<TResult>> operation,
        object? context = null,
        TimeSpan? timeout = null,
        int? maxAttempts = null,
        CancellationToken cancellationToken = default) =>
        await ExecuteWithRetryAsync(
            operation: operation,
            context: context,
            timeout: timeout,
            classifier: null,
            maxAttempts: maxAttempts ?? MaxRetryAttempts,
            cancellationToken: cancellationToken);

    private async Task<TResult> ExecuteWithRetryAsync<TResult>(
        Func<CancellationToken, Task<TResult>> operation,
        object? context = null,
        TimeSpan? timeout = null,
        IRetryClassifier? classifier = null,
        int? maxAttempts = null,
        CancellationToken cancellationToken = default)
    {
        var (kind, isIdempotent) = InferKindAndIdempotency(context);

        return await RetryExecutor.RunAsync(
            op: operation,
            policy: DefaultRetryPolicy,
            isIdempotent: isIdempotent,
            operationKind: kind,
            overallTimeout: timeout ?? DefaultTimeout,
            classifier: classifier,
            maxAttempts: maxAttempts ?? MaxRetryAttempts,
            ct: cancellationToken
        ).ConfigureAwait(false);
    }

    private static (OperationKind kind, bool isIdempotent) InferKindAndIdempotency(object? context)
    {
        switch (context)
        {
            case DbCommand cmd:
            {
                var kw = FirstKeyword(cmd.CommandText);
                return kw switch
                {
                    "SELECT" or "SHOW" or "DESCRIBE" => (OperationKind.Read,  true),
                    "INSERT" or "UPDATE" or "DELETE" or "UPSERT" => (OperationKind.Write, false),
                    "CREATE" or "DROP" or "ALTER" => (OperationKind.Schema, ContainsIfExists(cmd.CommandText)),
                    _ => (OperationKind.Read, false)
                };
            }
            case string sqlText:
            {
                var kw = FirstKeyword(sqlText);
                return kw switch
                {
                    "SELECT" or "SHOW" or "DESCRIBE" => (OperationKind.Read,  true),
                    "INSERT" or "UPDATE" or "DELETE" or "UPSERT" => (OperationKind.Write, false),
                    "CREATE" or "DROP" or "ALTER" => (OperationKind.Schema, ContainsIfExists(sqlText)),
                    _ => (OperationKind.Write, false)
                };
            }
        }

        return (OperationKind.Read, false);
    }
    
    private static bool ContainsIfExists(string? sql)
    {
        if (string.IsNullOrEmpty(sql)) return false;
            return sql.IndexOf("IF NOT EXISTS", StringComparison.OrdinalIgnoreCase) >= 0 
                   || sql.IndexOf("IF EXISTS", StringComparison.OrdinalIgnoreCase) >= 0;
    }

    private static string FirstKeyword(string? sql)
    {
        if (string.IsNullOrWhiteSpace(sql)) return string.Empty;
        var s = sql.AsSpan();

        while (s.Length > 0 && char.IsWhiteSpace(s[0])) s = s[1..];

        while (true)
        {
            if (s.StartsWith("--".AsSpan()))
            {
                var idx = s.IndexOfAny('\r', '\n');
                s = idx >= 0 ? s[(idx + 1)..] : ReadOnlySpan<char>.Empty;
                while (s.Length > 0 && char.IsWhiteSpace(s[0])) s = s[1..];
                continue;
            }
            if (s.StartsWith("/*".AsSpan()))
            {
                var end = s.IndexOf("*/".AsSpan());
                s = end >= 0 ? s[(end + 2)..] : ReadOnlySpan<char>.Empty;
                while (s.Length > 0 && char.IsWhiteSpace(s[0])) s = s[1..];
                continue;
            }
            break;
        }

        while (s.Length > 0 && s[0] == '(')
        {
            s = s[1..];
            while (s.Length > 0 && char.IsWhiteSpace(s[0])) s = s[1..];
        }

        var i = 0;
        while (i < s.Length && char.IsLetter(s[i])) i++;
        if (i == 0) return string.Empty;

        var kw = s[..i].ToString().ToUpperInvariant();
        if (kw == "WITH") return "SELECT";
        return kw;
    }
}

#endif
