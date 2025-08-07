using GrpcStatusCode = Grpc.Core.StatusCode;

namespace Ydb.Sdk.Retry;

internal sealed class YdbRetryPolicy : IRetryPolicy
{
    public const int DefaultMaxAttempts = 10;
    
    private static readonly TimeSpan FastStart = TimeSpan.FromMilliseconds(10);
    private static readonly TimeSpan FastCap = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan SlowStart = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan SlowCap = TimeSpan.FromSeconds(30);
    
    private const int HedgeStartAttempt = 3;
    
    private static readonly Random _rnd = Random.Shared;

    public RetryDecision Decide(in RetryContext ctx)
    {
        if (ctx.Operation == OperationKind.Stream)
            return new RetryDecision(null);
        
        if (ctx.DeadlineLeft is { } left && left <= TimeSpan.Zero)
            return new RetryDecision(null);

        var (tier, recreate, reset, hedge) = Classify(ctx);
        if (tier == BackoffTier.None)
            return new RetryDecision(null);

        var delay = tier switch
        {
            BackoffTier.Instant => TimeSpan.Zero,
            BackoffTier.Fast    => JitterDecorrelated(ctx.Attempt, FastStart, FastCap),
            BackoffTier.Slow    => JitterDecorrelated(ctx.Attempt, SlowStart, SlowCap),
            _ => TimeSpan.Zero
        };

        if (ctx.DeadlineLeft is { } dl && delay > dl - TimeSpan.FromMilliseconds(50))
            return new RetryDecision(null);

        return new RetryDecision(
            delay,
            recreate,
            reset,
            hedge && ctx.IsIdempotent && ctx.Attempt >= HedgeStartAttempt);
    }

    public void ReportResult(in RetryContext ctx, bool success)
    {
        System.Diagnostics.Debug.WriteLine(
            $"[Retry] op={ctx.Operation} attempt={ctx.Attempt} success={success} " +
            $"delayLeft={ctx.DeadlineLeft?.TotalMilliseconds:F0}ms " +
            $"exception={ctx.LastFailure?.Exception.GetType().Name ?? "—"}");
    }

    private (BackoffTier tier, bool recreate, bool reset, bool hedge) Classify(in RetryContext ctx)
    {
        var ydb  = ctx.LastFailure?.YdbStatusCode;
        var grpc = ctx.LastFailure?.GrpcStatusCode;

        const int BAD_SESSION     = 400100;
        const int SESSION_EXPIRED = 400150;
        const int SESSION_BUSY    = 400190;
        const int UNAVAILABLE     = 400050;
        const int ABORTED         = 400040;
        const int OVERLOADED      = 400060;
        const int TIMEOUT         = 400090;
        const int UNDETERMINED    = 400170;

        return ydb switch
        {
            BAD_SESSION or SESSION_EXPIRED => (BackoffTier.Instant, recreate: true, reset: false, hedge: false),
            SESSION_BUSY => (BackoffTier.Fast, recreate: true, reset: false, hedge: true),
            UNAVAILABLE or ABORTED => (BackoffTier.Fast, recreate: false, reset: false, hedge: true),
            OVERLOADED => (BackoffTier.Slow, recreate: false, reset: false, hedge: false),
            TIMEOUT or UNDETERMINED => ctx.IsIdempotent
                ? (BackoffTier.Fast, false, false, true)
                : (BackoffTier.None, false, false, false),
            _ => grpc switch
            {
                (int)GrpcStatusCode.Unavailable or (int)GrpcStatusCode.DeadlineExceeded => (BackoffTier.Fast,
                    recreate: false, reset: true, hedge: true),
                (int)GrpcStatusCode.ResourceExhausted => (BackoffTier.Slow, recreate: false, reset: false,
                    hedge: false),
                (int)GrpcStatusCode.Aborted => (BackoffTier.Instant, recreate: false, reset: false, hedge: true),
                _ => (BackoffTier.None, false, false, false)
            }
        };
    }

    private static TimeSpan JitterDecorrelated(int attempt, TimeSpan start, TimeSpan cap)
    {
        if (attempt <= 1) return TimeSpan.Zero;

        var safeAttempt = Math.Min(attempt, 30);
        var exp = Math.Pow(2, safeAttempt - 2);
        var maxMs = Math.Min(cap.TotalMilliseconds, start.TotalMilliseconds * exp);
        var minMs = start.TotalMilliseconds;
        var jitterMs = _rnd.NextDouble() * (maxMs - minMs);

        return TimeSpan.FromMilliseconds(minMs + jitterMs);
    }
}
