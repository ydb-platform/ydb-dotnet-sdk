using System.Data.Common;

namespace Ydb.Sdk.Ado.Retry;

public interface IRetryPolicy
{
    int MaxAttempts { get; }

    bool CanRetry(Exception ex, bool isIdempotent);

    TimeSpan? GetDelay(Exception ex, int attempt);

    bool IsStreaming(DbCommand command, System.Data.CommandBehavior behavior);
}
