namespace Ydb.Sdk.Ado.Retry;

public sealed class DefaultRetryPolicy : IRetryPolicy
{
    private readonly RetryConfig _cfg;

    public DefaultRetryPolicy(RetryConfig? config = null)
        => _cfg = config ?? new RetryConfig();

    public int MaxAttempts => _cfg.MaxAttempts;

    public bool CanRetry(Exception ex, bool isIdempotent)
    {
        if (ex is YdbException ydb)
            return isIdempotent ? ydb.IsTransientWhenIdempotent : ydb.IsTransient;

        return false;
    }

    public TimeSpan? GetDelay(Exception ex, int attempt)
    {
        if (attempt <= 0) attempt = 1;

        if (ex is YdbException ydb)
        {
            if (_cfg.PerStatusDelay.TryGetValue(ydb.Code, out var calc))
                return Cap(calc(attempt));

            var profileDelay = _cfg.StatusDelayProfile.GetDelay(ydb.Code, attempt);
            if (profileDelay is not null)
                return Cap(profileDelay);
        }

        return Cap(_cfg.DefaultDelay(ex, attempt));
    }

    private TimeSpan? Cap(TimeSpan? delay)
    {
        if (delay is null) return null;
        if (delay.Value <= TimeSpan.Zero) return TimeSpan.Zero;
        return delay.Value <= _cfg.MaxDelay ? delay : _cfg.MaxDelay;
    }
}
