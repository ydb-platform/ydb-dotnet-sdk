namespace Ydb.Sdk.Retry;

internal interface IRetryPolicy
{
    RetryDecision Decide(in RetryContext ctx);

    void ReportResult(in RetryContext ctx, bool success);
}
