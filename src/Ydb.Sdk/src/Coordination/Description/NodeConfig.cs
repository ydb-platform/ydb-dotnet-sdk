using Ydb.Coordination;

namespace Ydb.Sdk.Coordination.Description;

public readonly struct NodeConfig
{
    /// <summary>Period for self-checks (default 1 second)</summary>
    private TimeSpan SelfCheckPeriod { get; }

    /// <summary>Grace period for sessions on leader change (default 10 seconds)</summary>
    private TimeSpan SessionGracePeriod { get; }

    /// <summary>Consistency mode for read operations</summary>
    private ConsistencyMode ReadConsistencyMode { get; }

    /// <summary>Consistency mode for attach operations</summary>
    private ConsistencyMode AttachConsistencyMode { get; }

    /// <summary>Rate limiter counters mode</summary>
    private RateLimiterCountersMode RateLimiterCountersModeValue { get; }


    public NodeConfig()
    {
        SelfCheckPeriod = TimeSpan.FromSeconds(1);
        SessionGracePeriod = TimeSpan.FromSeconds(10);
        ReadConsistencyMode = ConsistencyMode.Unset;
        AttachConsistencyMode = ConsistencyMode.Unset;
        RateLimiterCountersModeValue = RateLimiterCountersMode.Unset;
    }

    private NodeConfig(DescribeNodeResult result)
    {
        //Preconditions.checkNotNull(result, "DescriptionNodeResult must be not null");
        //Preconditions.checkNotNull(result.getConfig(), "DescriptionNodeResult config must be not null");

        SelfCheckPeriod = TimeSpan.FromMilliseconds(result.Config.SelfCheckPeriodMillis);
        SessionGracePeriod = TimeSpan.FromMilliseconds(result.Config.SessionGracePeriodMillis);
        ReadConsistencyMode = FromProto(result.Config.ReadConsistencyMode);
        AttachConsistencyMode = FromProto(result.Config.AttachConsistencyMode);
        RateLimiterCountersModeValue = FromProto(result.Config.RateLimiterCountersMode);
    }


    private NodeConfig(TimeSpan selfCheck, TimeSpan sessionGrace, ConsistencyMode read, ConsistencyMode attach,
        RateLimiterCountersMode rateLimiter)
    {
        /*
        Preconditions.checkArgument(!selfCheck.isNegative() && !selfCheck.isZero(),
            "SelfCheckPeriod must be strictly greater than zero"
        );
        Preconditions.checkArgument(!sessionGrace.isNegative() && !sessionGrace.isZero(),
            "SessionGracePeriod must be strictly greater than zero"
        );
        Preconditions.checkArgument(sessionGrace.compareTo(selfCheck) > 0,
            "SessionGracePeriod must be strictly more than SelfCheckPeriod"
        );
        */
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
