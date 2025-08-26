namespace Ydb.Sdk.Ado.RetryPolicy;

public interface IRetryPolicy
{
    public TimeSpan? GetNextDelay(YdbException ydbException, int attempt);
}
