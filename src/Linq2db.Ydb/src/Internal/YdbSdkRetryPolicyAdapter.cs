using L2RetryOptions = LinqToDB.Data.RetryPolicy.RetryPolicyOptions;
using L2IRetryPolicy = LinqToDB.Data.RetryPolicy.IRetryPolicy;
using Ydb.Sdk.Ado; // <== важно: тут YdbException
using Ydb.Sdk.Ado.RetryPolicy;

namespace LinqToDB.Internal.DataProvider.Ydb.Internal
{
    /// <summary>
    /// Адаптер YDB SDK retry policy под интерфейс ретраев linq2db.
    /// </summary>
    public sealed class YdbSdkRetryPolicyAdapter : L2IRetryPolicy
    {
        private readonly YdbRetryPolicy _inner;
        private readonly Action<int, Exception, TimeSpan?>? _onRetry;

        public YdbSdkRetryPolicyAdapter(
            YdbRetryPolicyConfig? config = null,
            Action<int, Exception, TimeSpan?>? onRetry = null)
        {
            _inner  = new YdbRetryPolicy(config ?? YdbRetryPolicyConfig.Default);
            _onRetry = onRetry;
        }

        // ---------- Sync ----------
        public TResult Execute<TResult>(Func<TResult> operation)
        {
            if (operation == null) throw new ArgumentNullException(nameof(operation));

            var attempt = 0;
            while (true)
            {
                try
                {
                    return operation();
                }
                catch (Exception ex) when (TryGetDelay(ex, attempt, out var delay))
                {
                    _onRetry?.Invoke(attempt, ex, delay);
                    if (delay > TimeSpan.Zero)
                        Thread.Sleep(delay);
                    attempt++;
                }
            }
        }

        public void Execute(Action operation)
        {
            Execute<object?>(() =>
            {
                operation();
                return null;
            });
        }

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
                    _onRetry?.Invoke(attempt, ex, delay);
                    if (delay > TimeSpan.Zero)
                        await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
                    attempt++;
                }
            }
        }

        public async Task ExecuteAsync(
            Func<CancellationToken, Task> operation,
            CancellationToken cancellationToken = default)
        {
            await ExecuteAsync<object?>(async ct =>
            {
                await operation(ct).ConfigureAwait(false);
                return null;
            }, cancellationToken).ConfigureAwait(false);
        }

        // ---------- Helpers ----------
        private bool TryGetDelay(Exception ex, int attempt, out TimeSpan delay)
        {
            delay = default;

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
                cur = cur.InnerException!;
            }

            ydbEx = null!;
            return false;
        }
    }

    /// <summary>
    /// Утилиты для подключения политики к linq2db.
    /// </summary>
    public static class YdbSdkRetryPolicyRegistration
    {
        public static void UseGlobally(YdbRetryPolicyConfig? config = null, Action<int, Exception, TimeSpan?>? onRetry = null)
        {
            L2RetryOptions.Default = L2RetryOptions.Default with
            {
                Factory = (_ /*DataConnection dc*/) => new YdbSdkRetryPolicyAdapter(config ?? YdbRetryPolicyConfig.Default, onRetry)
            };
        }

        /// <summary>
        /// Вариант с идемпотентностью (создаёт новый конфиг, т.к. YdbRetryPolicyConfig — не record).
        /// </summary>
        public static void UseGloballyWithIdempotence(
            int? maxAttempts = null,
            int? fastBaseMs = null, int? slowBaseMs = null,
            int? fastCapMs = null,  int? slowCapMs = null,
            Action<int, Exception, TimeSpan?>? onRetry = null)
        {
            var cfg = new YdbRetryPolicyConfig
            {
                EnableRetryIdempotence = true,
                MaxAttempts       = maxAttempts   ?? YdbRetryPolicyConfig.Default.MaxAttempts,
                FastBackoffBaseMs = fastBaseMs    ?? YdbRetryPolicyConfig.Default.FastBackoffBaseMs,
                SlowBackoffBaseMs = slowBaseMs    ?? YdbRetryPolicyConfig.Default.SlowBackoffBaseMs,
                FastCapBackoffMs  = fastCapMs     ?? YdbRetryPolicyConfig.Default.FastCapBackoffMs,
                SlowCapBackoffMs  = slowCapMs     ?? YdbRetryPolicyConfig.Default.SlowCapBackoffMs
            };

            UseGlobally(cfg, onRetry);
        }
    }
}
