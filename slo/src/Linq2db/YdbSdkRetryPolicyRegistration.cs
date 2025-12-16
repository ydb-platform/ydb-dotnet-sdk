using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.RetryPolicy;
using L2RetryOptions = LinqToDB.Data.RetryPolicy.RetryPolicyOptions;
using L2IRetryPolicy = LinqToDB.Data.RetryPolicy.IRetryPolicy;

namespace Linq2db;

public sealed class YdbSdkRetryPolicyAdapter(
    YdbRetryPolicyConfig? config = null,
    Action<int, Exception, TimeSpan?>? onRetry = null
) : L2IRetryPolicy
{
    private readonly YdbRetryPolicy _inner = new(config ?? YdbRetryPolicyConfig.Default);

    public TResult Execute<TResult>(Func<TResult> operation)
    {
        var attempt = 0;
        while (true)
        {
            try
            {
                return operation();
            }
            catch (Exception ex) when (TryGetDelay(ex, attempt, out var delay))
            {
                onRetry?.Invoke(attempt, ex, delay);
                if (delay > TimeSpan.Zero)
                    Thread.Sleep(delay);
                attempt++;
            }
        }
    }

    public void Execute(Action operation) => Execute<object?>(() =>
    {
        operation();
        return null;
    });

    // ---------- Async ----------
    public async Task<TResult> ExecuteAsync<TResult>(
        Func<CancellationToken, Task<TResult>> operation,
        CancellationToken cancellationToken = default)
    {
        if (operation == null) throw new ArgumentNullException(nameof(operation));

        var attempt = 0;
        while (true)
        {
            try
            {
                return await operation(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (TryGetDelay(ex, attempt, out var delay))
            {
                onRetry?.Invoke(attempt, ex, delay);
                if (delay > TimeSpan.Zero)
                    await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                attempt++;
            }
        }
    }

    public async Task ExecuteAsync(
        Func<CancellationToken, Task> operation,
        CancellationToken cancellationToken = default) =>
        await ExecuteAsync<object?>(async ct =>
        {
            await operation(ct).ConfigureAwait(false);
            return null;
        }, cancellationToken).ConfigureAwait(false);

    // ---------- Helpers ----------
    private bool TryGetDelay(Exception ex, int attempt, out TimeSpan delay)
    {
        delay = TimeSpan.Zero;

        if (!TryFindYdbException(ex, out var ydbEx))
            return false;

        var next = _inner.GetNextDelay(ydbEx, attempt);
        if (next is null)
            return false;

        delay = next.Value;
        return true;
    }

    private static bool TryFindYdbException(Exception ex, out YdbException ydbEx)
    {
        var cur = ex;
        while (cur != null)
        {
            if (cur is YdbException ye)
            {
                ydbEx = ye;
                return true;
            }

            cur = cur.InnerException;
        }

        ydbEx = null!;
        return false;
    }
}

public static class YdbSdkRetryPolicyRegistration
{
    private static void UseGlobally(YdbRetryPolicyConfig? config = null,
        Action<int, Exception, TimeSpan?>? onRetry = null) =>
        L2RetryOptions.Default = L2RetryOptions.Default with
        {
            Factory = _ => new YdbSdkRetryPolicyAdapter(config ?? YdbRetryPolicyConfig.Default, onRetry)
        };

    public static void UseGloballyWithIdempotence(
        int? maxAttempts = null,
        int? fastBaseMs = null, int? slowBaseMs = null,
        int? fastCapMs = null, int? slowCapMs = null,
        Action<int, Exception, TimeSpan?>? onRetry = null)
    {
        var cfg = new YdbRetryPolicyConfig
        {
            EnableRetryIdempotence = true,
            MaxAttempts = maxAttempts ?? YdbRetryPolicyConfig.Default.MaxAttempts,
            FastBackoffBaseMs = fastBaseMs ?? YdbRetryPolicyConfig.Default.FastBackoffBaseMs,
            SlowBackoffBaseMs = slowBaseMs ?? YdbRetryPolicyConfig.Default.SlowBackoffBaseMs,
            FastCapBackoffMs = fastCapMs ?? YdbRetryPolicyConfig.Default.FastCapBackoffMs,
            SlowCapBackoffMs = slowCapMs ?? YdbRetryPolicyConfig.Default.SlowCapBackoffMs
        };

        UseGlobally(cfg, onRetry);
    }
}