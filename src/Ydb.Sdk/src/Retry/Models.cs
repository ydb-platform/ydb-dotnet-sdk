namespace Ydb.Sdk.Retry;

internal enum OperationKind
{
    Read,
    Write,
    Schema,
    Stream,
    TopicProduce,
    TopicConsume,
    Discovery
}

internal enum BackoffTier
{
    None,
    Instant,
    Fast,
    Slow
}

internal readonly record struct Failure(
    Exception Exception,
    int? YdbStatusCode = null,
    int? GrpcStatusCode = null
);

internal readonly record struct RetryContext(
    int Attempt,
    TimeSpan Elapsed,
    TimeSpan? DeadlineLeft,
    bool IsIdempotent,
    OperationKind Operation,
    Failure? LastFailure
);

internal readonly record struct RetryDecision(
    TimeSpan? Delay,
    bool RecreateSession = false,
    bool ResetTransport = false,
    bool Hedge = false
);
