using Ydb.Coordination;

namespace Ydb.Sdk.Coordination.Description;

public readonly struct NodeConfig
{
    /// <summary>
    /// Period in milliseconds for self-checks (default 1 second).
    /// </summary>
    public TimeSpan SelfCheckPeriod { get; init; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Grace period for sessions on leader change (default 10 seconds).
    /// </summary>
    public TimeSpan SessionGracePeriod { get; init; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Consistency mode for read operations.
    /// </summary>
    public ConsistencyMode ReadConsistencyMode { get; init; } = ConsistencyMode.Unset;

    /// <summary>
    /// Consistency mode for attach operations.
    /// </summary>
    public ConsistencyMode AttachConsistencyMode { get; init; } = ConsistencyMode.Unset;

    /// <summary>
    /// Rate limiter counters mode.
    /// </summary>
    public RateLimiterCountersMode RateLimiterCountersModeValue { get; init; } = RateLimiterCountersMode.Unset;

    private NodeConfig(DescribeNodeResult result)
    {
        SelfCheckPeriod = TimeSpan.FromMilliseconds(result.Config.SelfCheckPeriodMillis);
        SessionGracePeriod = TimeSpan.FromMilliseconds(result.Config.SessionGracePeriodMillis);
        ReadConsistencyMode = FromProto(result.Config.ReadConsistencyMode);
        AttachConsistencyMode = FromProto(result.Config.AttachConsistencyMode);
        RateLimiterCountersModeValue = FromProto(result.Config.RateLimiterCountersMode);
    }

    public Config ToProto() => new()
    {
        SelfCheckPeriodMillis = (uint)SelfCheckPeriod.TotalMilliseconds,
        SessionGracePeriodMillis = (uint)SessionGracePeriod.TotalMilliseconds,
        ReadConsistencyMode = ToProto(ReadConsistencyMode), AttachConsistencyMode = ToProto(AttachConsistencyMode),
        RateLimiterCountersMode = ToProto(RateLimiterCountersModeValue)
    };


    internal static NodeConfig FromProto(DescribeNodeResult result) => new(result);

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
