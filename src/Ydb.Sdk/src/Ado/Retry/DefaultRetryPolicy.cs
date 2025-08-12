using System.Data;
using System.Data.Common;

namespace Ydb.Sdk.Ado.Retry;

public sealed class DefaultRetryPolicy : IRetryPolicy
{
    private readonly RetryConfig _cfg;

    public DefaultRetryPolicy(RetryConfig? config = null) => _cfg = config ?? new RetryConfig();

    public int MaxAttempts => _cfg.MaxAttempts;

    public bool IsStreaming(DbCommand command, CommandBehavior behavior) => _cfg.IsStreaming(command, behavior);

    public bool CanRetry(Exception ex, bool isIdempotent)
    {
        if (TryUnwrapYdbException(ex, out var ydb))
        {
            return isIdempotent ? ydb.IsTransientWhenIdempotent : ydb.IsTransient;
        }

        if (ex is TimeoutException) return true;
        if (ex is OperationCanceledException oce && !oce.CancellationToken.IsCancellationRequested) return true;

        return false;
    }

    public TimeSpan? GetDelay(Exception ex, int attempt)
    {
        if (attempt <= 0) attempt = 1;

        if (TryUnwrapYdbException(ex, out var ydb))
        {
            if (_cfg.PerStatusDelay.TryGetValue(ydb.Code, out var calc))
                return Cap(calc(attempt));
        }

        return Cap(_cfg.DefaultDelay(ex, attempt));
    }

    private static bool TryUnwrapYdbException(Exception ex, out YdbException ydb)
    {
        for (var e = ex; e is not null; e = e.InnerException!)
        {
            if (e is YdbException yy)
            {
                ydb = yy; return true;
            }
        }
        ydb = null!;
        return false;
    }

    private TimeSpan? Cap(TimeSpan? delay)
    {
        if (delay is null) return null;
        if (delay.Value <= TimeSpan.Zero) return TimeSpan.Zero;
        return delay.Value <= _cfg.MaxDelay ? delay : _cfg.MaxDelay;
    }
}
