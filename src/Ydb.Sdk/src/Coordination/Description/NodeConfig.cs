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

    /// <summary>Consistency mode for attach operations.</summary>
    public ConsistencyMode AttachConsistencyMode { get; init; } = ConsistencyMode.Unset;

    /// <summary>Rate limiter counters mode.</summary>
    public RateLimiterCountersMode RateLimiterCountersModeValue { get; init; } = RateLimiterCountersMode.Unset;

    internal NodeConfig(DescribeNodeResult result)
    {
        SelfCheckPeriod = TimeSpan.FromMilliseconds(result.Config.SelfCheckPeriodMillis);
        SessionGracePeriod = TimeSpan.FromMilliseconds(result.Config.SessionGracePeriodMillis);
        ReadConsistencyMode = result.Config.ReadConsistencyMode.ToSdk();
        AttachConsistencyMode = result.Config.AttachConsistencyMode.ToSdk();
        RateLimiterCountersModeValue = result.Config.RateLimiterCountersMode.ToSdk();
    }

    internal Config ToProto() => new()
    {
        SelfCheckPeriodMillis = (uint)SelfCheckPeriod.TotalMilliseconds,
        SessionGracePeriodMillis = (uint)SessionGracePeriod.TotalMilliseconds,
        ReadConsistencyMode = ReadConsistencyMode.ToProto(),
        AttachConsistencyMode = AttachConsistencyMode.ToProto(),
        RateLimiterCountersMode = RateLimiterCountersModeValue.ToProto()
    };
}

internal static class NodeConfigEnumMapping
{
    internal static Ydb.Coordination.ConsistencyMode ToProto(this ConsistencyMode mode) =>
        Enum.IsDefined(typeof(Ydb.Coordination.ConsistencyMode), (int)mode)
            ? (Ydb.Coordination.ConsistencyMode)(int)mode
            : Ydb.Coordination.ConsistencyMode.Unset;

    internal static ConsistencyMode ToSdk(this Ydb.Coordination.ConsistencyMode mode) =>
        Enum.IsDefined(typeof(ConsistencyMode), (int)mode)
            ? (ConsistencyMode)(int)mode
            : ConsistencyMode.Unset;

    internal static Ydb.Coordination.RateLimiterCountersMode ToProto(this RateLimiterCountersMode mode) =>
        Enum.IsDefined(typeof(Ydb.Coordination.RateLimiterCountersMode), (int)mode)
            ? (Ydb.Coordination.RateLimiterCountersMode)(int)mode
            : Ydb.Coordination.RateLimiterCountersMode.Unset;

    internal static RateLimiterCountersMode ToSdk(this Ydb.Coordination.RateLimiterCountersMode mode) =>
        Enum.IsDefined(typeof(RateLimiterCountersMode), (int)mode)
            ? (RateLimiterCountersMode)(int)mode
            : RateLimiterCountersMode.Unset;
}
