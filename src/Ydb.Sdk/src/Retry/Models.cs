namespace Ydb.Sdk.Retry;

public enum OperationKind
{
    Read,
    Write,
    Schema,
    Stream,
    TopicProduce,
    TopicConsume,
    Discovery
}

public enum BackoffTier
{
    None,
    Instant,
    Fast,
    Slow
}

public readonly record struct Failure(
    Exception Exception,
    int? YdbStatusCode = null,
    int? GrpcStatusCode = null
);

public readonly record struct RetryContext(
    int Attempt,
    TimeSpan Elapsed,
    TimeSpan? DeadlineLeft,
    bool IsIdempotent,
    OperationKind Operation,
    Failure? LastFailure
);

public readonly record struct RetryDecision(
    TimeSpan? Delay,
    bool RecreateSession = false,
    bool ResetTransport = false,
    bool Hedge = false
);