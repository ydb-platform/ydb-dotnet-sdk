using Ydb.Coordination;

namespace Ydb.Sdk.Coordination.Description;

public readonly struct NodeConfig
{
    /// Period in milliseconds for self-checks (default 1 second).
    private TimeSpan SelfCheckPeriod { get; init; } = TimeSpan.FromSeconds(1);
    
    /// Grace period for sessions on leader change (default 10 seconds).
    private TimeSpan SessionGracePeriod { get; init; } = TimeSpan.FromSeconds(10);
    
    /// Consistency mode for read operations.
    private ConsistencyMode ReadConsistencyMode { get; init; } = ConsistencyMode.Unset;
    
    /// Consistency mode for attach operations.
    private ConsistencyMode AttachConsistencyMode { get; init; } = ConsistencyMode.Unset;
    
    /// Rate limiter counters mode.
    private RateLimiterCountersMode RateLimiterCountersModeValue { get; init; } = RateLimiterCountersMode.Unset;

    private NodeConfig(DescribeNodeResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        ArgumentNullException.ThrowIfNull(result.Config);

        SelfCheckPeriod = TimeSpan.FromMilliseconds(result.Config.SelfCheckPeriodMillis);
        SessionGracePeriod = TimeSpan.FromMilliseconds(result.Config.SessionGracePeriodMillis);
        ReadConsistencyMode = FromProto(result.Config.ReadConsistencyMode);
        AttachConsistencyMode = FromProto(result.Config.AttachConsistencyMode);
        RateLimiterCountersModeValue = FromProto(result.Config.RateLimiterCountersMode);
    }


    private NodeConfig(TimeSpan selfCheck, TimeSpan sessionGrace, ConsistencyMode read, ConsistencyMode attach,
        RateLimiterCountersMode rateLimiter)
    {
        if (selfCheck <= TimeSpan.Zero)
            throw new ArgumentException("SelfCheckPeriod must be strictly greater than zero");
        if (sessionGrace <= TimeSpan.Zero)
            throw new ArgumentException("SessionGracePeriod must be strictly greater than zero");
        if (sessionGrace <= selfCheck)
            throw new ArgumentException("SessionGracePeriod must be strictly more than SelfCheckPeriod");
        SelfCheckPeriod = selfCheck;
        SessionGracePeriod = sessionGrace;
        ReadConsistencyMode = read;
        AttachConsistencyMode = attach;
        RateLimiterCountersModeValue = rateLimiter;
    }

    public NodeConfig WithDurationsConfig(TimeSpan selfCheck, TimeSpan sessionGrace)
        => new(
            selfCheck, sessionGrace, ReadConsistencyMode, AttachConsistencyMode, RateLimiterCountersModeValue
        );


    public NodeConfig WithReadConsistencyMode(ConsistencyMode mode)
        => new(
            SelfCheckPeriod, SessionGracePeriod, mode, AttachConsistencyMode, RateLimiterCountersModeValue
        );


    public NodeConfig WithAttachConsistencyMode(ConsistencyMode mode)
        => new(
            SelfCheckPeriod, SessionGracePeriod, ReadConsistencyMode, mode, RateLimiterCountersModeValue
        );


    public NodeConfig WithRateLimiterCountersMode(RateLimiterCountersMode mode)
        => new(
            SelfCheckPeriod, SessionGracePeriod, ReadConsistencyMode, AttachConsistencyMode, mode
        );

    public Config ToProto()
        => new()
        {
            SelfCheckPeriodMillis = (uint)SelfCheckPeriod.TotalMilliseconds,
            SessionGracePeriodMillis = (uint)SessionGracePeriod.TotalMilliseconds,
            ReadConsistencyMode = ToProto(ReadConsistencyMode),
            AttachConsistencyMode = ToProto(AttachConsistencyMode),
            RateLimiterCountersMode = ToProto(RateLimiterCountersModeValue)
        };

    public static NodeConfig Create()
        => new();

    public static NodeConfig FromProto(DescribeNodeResult result)
        => new(result);

    private static Ydb.Coordination.ConsistencyMode ToProto(ConsistencyMode mode)
    {
        switch (mode)
        {
            case ConsistencyMode.Strict:
                return Ydb.Coordination.ConsistencyMode.Strict;
            case ConsistencyMode.Relaxed:
                return Ydb.Coordination.ConsistencyMode.Relaxed;
            case ConsistencyMode.Unset:
            default:
                return Ydb.Coordination.ConsistencyMode.Unset;
        }
    }

    private static Ydb.Coordination.RateLimiterCountersMode ToProto(RateLimiterCountersMode mode)
    {
        switch (mode)
        {
            case RateLimiterCountersMode.Detailed:
                return Ydb.Coordination.RateLimiterCountersMode.Detailed;
            case RateLimiterCountersMode.Aggregated:
                return Ydb.Coordination.RateLimiterCountersMode.Aggregated;
            case RateLimiterCountersMode.Unset:
            default:
                return Ydb.Coordination.RateLimiterCountersMode.Unset;
        }
    }

    private static ConsistencyMode FromProto(Ydb.Coordination.ConsistencyMode mode)
    {
        switch (mode)
        {
            case Ydb.Coordination.ConsistencyMode.Relaxed:
                return ConsistencyMode.Relaxed;
            case Ydb.Coordination.ConsistencyMode.Strict:
                return ConsistencyMode.Strict;
            case Ydb.Coordination.ConsistencyMode.Unset:
            default:
                return ConsistencyMode.Unset;
        }
    }

    private static RateLimiterCountersMode FromProto(Ydb.Coordination.RateLimiterCountersMode mode)
    {
        switch (mode)
        {
            case Ydb.Coordination.RateLimiterCountersMode.Aggregated:
                return RateLimiterCountersMode.Aggregated;
            case Ydb.Coordination.RateLimiterCountersMode.Detailed:
                return RateLimiterCountersMode.Detailed;
            case Ydb.Coordination.RateLimiterCountersMode.Unset:
            default:
                return RateLimiterCountersMode.Unset;
        }
    }
}
