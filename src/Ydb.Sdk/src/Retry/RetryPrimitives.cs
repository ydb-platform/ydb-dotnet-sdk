namespace Ydb.Sdk.Retry;

public static class RetryPrimitives
{
    public static IRetryPolicy NoRetry { get; } = new NoRetryPolicy();

    public static IRetryPolicy FixedDelay(TimeSpan delay, int maxAttempts = 3) =>
        new FixedDelayPolicy(delay, maxAttempts);

    private sealed class NoRetryPolicy : IRetryPolicy
    {
        public RetryDecision Decide(in RetryContext ctx) => new(null);
        public void ReportResult(in RetryContext ctx, bool success) { }
    }

    private sealed class FixedDelayPolicy : IRetryPolicy
    {
        private readonly TimeSpan _delay;
        private readonly int _maxAttempts;

        public FixedDelayPolicy(TimeSpan delay, int maxAttempts)
        {
            _delay = delay;
            _maxAttempts = maxAttempts;
        }

        public RetryDecision Decide(in RetryContext ctx)
        {
            return ctx.Attempt < _maxAttempts
                ? new RetryDecision(_delay)
                : new RetryDecision(null);
        }

        public void ReportResult(in RetryContext ctx, bool success) { }
    }
}