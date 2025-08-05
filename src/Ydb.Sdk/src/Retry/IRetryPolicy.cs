namespace Ydb.Sdk.Retry;

public interface IRetryPolicy
{
    RetryDecision Decide(in RetryContext ctx);

    void ReportResult(in RetryContext ctx, bool success);
}