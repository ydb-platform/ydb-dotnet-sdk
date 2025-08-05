namespace Ydb.Sdk.Retry;

public sealed class YdbRetryPolicy : IRetryPolicy
{
    private static readonly TimeSpan FastStart = TimeSpan.FromMilliseconds(10);
    private static readonly TimeSpan FastCap = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan SlowStart = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan SlowCap = TimeSpan.FromSeconds(30);
    private const double Mult = 2.0;

    public RetryDecision Decide(in RetryContext ctx)
    {
        if (ctx.DeadlineLeft is { } left && left <= TimeSpan.Zero)
            return new(null);

        var (tier, recreate, reset, hedge) = Classify(ctx);
        if (tier == BackoffTier.None)
            return new(null);

        var delay = tier switch
        {
            BackoffTier.Instant => TimeSpan.Zero,
            BackoffTier.Fast    => Jitter(ctx.Attempt, FastStart, FastCap),
            BackoffTier.Slow    => Jitter(ctx.Attempt, SlowStart, SlowCap),
            _ => TimeSpan.Zero
        };

        // чтобы не выходить за край дедлайна (с небольшим запасом)
        if (ctx.DeadlineLeft is { } dl && delay > dl - TimeSpan.FromMilliseconds(50))
            return new(null);

        return new(delay, recreate, reset, hedge && ctx.IsIdempotent && ctx.Attempt >= 2);
    }

    public void ReportResult(in RetryContext ctx, bool success)
    {
        // Пока ничего — можно добавить метрики/логику адаптации в будущем.
    }

    private (BackoffTier tier, bool recreate, bool reset, bool hedge) Classify(in RetryContext ctx)
    {
        var ydb  = ctx.LastFailure?.YdbStatusCode;
        var grpc = ctx.LastFailure?.GrpcStatusCode;

        // Всё вынесено в константы для читабельности и повторного использования
        const int badSession     = 400100;
        const int sessionExpired = 400150;
        const int sessionBusy    = 400190;
        const int unavailable     = 400050;
        const int aborted         = 400040;
        const int overloaded      = 400060;
        const int timeout         = 400090;
        const int undetermined    = 400170;

        switch (ydb)
        {
            case badSession or sessionExpired:
                return (BackoffTier.Instant, recreate: true,  reset: false, hedge: false);
            case sessionBusy:
                return (BackoffTier.Fast,    recreate: true,  reset: false, hedge: true);
            case unavailable or aborted:
                return (BackoffTier.Fast,    recreate: false, reset: false, hedge: true);
            case overloaded:
                return (BackoffTier.Slow,    recreate: false, reset: false, hedge: false);
            case timeout or undetermined:
                return ctx.IsIdempotent ? (BackoffTier.Fast, false, false, true)
                    : (BackoffTier.None, false, false, false);
        }

        switch (grpc)
        {
            case (int)Grpc.Core.StatusCode.Unavailable or (int)Grpc.Core.StatusCode.DeadlineExceeded:
                return (BackoffTier.Fast, recreate: false, reset: true,  hedge: true);
            case (int)Grpc.Core.StatusCode.ResourceExhausted:
                return (BackoffTier.Slow, recreate: false, reset: false, hedge: false);
            case (int)Grpc.Core.StatusCode.Aborted:
                return (BackoffTier.Instant, recreate: false, reset: false, hedge: true);
            default:
                return (BackoffTier.None, false, false, false);
        }
    }

    private static TimeSpan Jitter(int attempt, TimeSpan start, TimeSpan cap)
    {
        var max = Math.Min(cap.TotalMilliseconds, Math.Pow(Mult, attempt - 1) * start.TotalMilliseconds);
        if (max < start.TotalMilliseconds) max = start.TotalMilliseconds;
        var next = Random.Shared.NextDouble() * (max - start.TotalMilliseconds) + start.TotalMilliseconds;
        return TimeSpan.FromMilliseconds(next);
    }
}