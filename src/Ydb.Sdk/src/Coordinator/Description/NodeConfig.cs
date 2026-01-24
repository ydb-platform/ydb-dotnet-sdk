using Ydb.Coordination;

namespace Ydb.Sdk.Coordinator.Description;

public class NodeConfig
{
    public enum ConsistencyMode
    {
        /// <summary>The default or current value</summary>
        Unset,

        /// <summary>Strict mode makes sure operations may only complete on current leader</summary>
        Strict,

        /// <summary>Relaxed mode allows operations to complete on stale masters</summary>
        Relaxed
    }

    public enum RateLimiterCountersMode
    {
        /// <summary>The default or current value</summary>
        Unset,

        /// <summary>Aggregated counters for resource tree</summary>
        Aggregated,

        /// <summary>Counters on every resource</summary>
        Detailed
    }

    /// <summary>Period for self-checks (default 1 second)</summary>
    public TimeSpan SelfCheckPeriod { get; }

    /// <summary>Grace period for sessions on leader change (default 10 seconds)</summary>
    public TimeSpan SessionGracePeriod { get; }

    /// <summary>Consistency mode for read operations</summary>
    public ConsistencyMode ReadConsistencyMode { get; }

    /// <summary>Consistency mode for attach operations</summary>
    public ConsistencyMode AttachConsistencyMode { get; }

    /// <summary>Rate limiter counters mode</summary>
    public RateLimiterCountersMode RateLimiterCountersModeValue { get; }


    private NodeConfig()
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
        SelfCheckPeriod = selfCheck;
        SessionGracePeriod = sessionGrace;
        ReadConsistencyMode = read;
        AttachConsistencyMode = attach;
        RateLimiterCountersModeValue = rateLimiter;
    }

    public NodeConfig WithDurationsConfig(TimeSpan selfCheck, TimeSpan sessionGrace)
        => new NodeConfig(
            selfCheck, sessionGrace, ReadConsistencyMode, AttachConsistencyMode, RateLimiterCountersModeValue
        );


    public NodeConfig WithReadConsistencyMode(ConsistencyMode mode)
        => new NodeConfig(
            SelfCheckPeriod, SessionGracePeriod, mode, AttachConsistencyMode, RateLimiterCountersModeValue
        );


    public NodeConfig WithAttachConsistencyMode(ConsistencyMode mode)
        => new NodeConfig(
            SelfCheckPeriod, SessionGracePeriod, ReadConsistencyMode, mode, RateLimiterCountersModeValue
        );


    public NodeConfig WithRateLimiterCountersMode(RateLimiterCountersMode mode)
        => new NodeConfig(
            SelfCheckPeriod, SessionGracePeriod, ReadConsistencyMode, AttachConsistencyMode, mode
        );

    public Config ToProto()
        => new Config
        {
            SelfCheckPeriodMillis = (uint)SelfCheckPeriod.TotalMilliseconds,
            SessionGracePeriodMillis = (uint)SessionGracePeriod.TotalMilliseconds,
            ReadConsistencyMode = ToProto(ReadConsistencyMode),
            AttachConsistencyMode = ToProto(AttachConsistencyMode),
            RateLimiterCountersMode = ToProto(RateLimiterCountersModeValue)
        };

    public static NodeConfig Create()
        => new NodeConfig();

    public static NodeConfig FromProto(DescribeNodeResult result)
        => new NodeConfig(result);

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
