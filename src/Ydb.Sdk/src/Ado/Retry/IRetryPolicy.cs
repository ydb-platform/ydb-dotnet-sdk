namespace Ydb.Sdk.Ado.Retry;

public interface IRetryPolicy
{
    int MaxAttempts { get; }

    bool CanRetry(Exception ex, bool isIdempotent);

    TimeSpan? GetDelay(Exception ex, int attempt);
}
