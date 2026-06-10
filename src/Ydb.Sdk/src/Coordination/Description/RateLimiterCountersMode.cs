namespace Ydb.Sdk.Coordination.Description;

public enum RateLimiterCountersMode
{
    /// <summary>
    /// The default or current value.
    /// </summary>
    Unset = Ydb.Coordination.RateLimiterCountersMode.Unset,

    /// <summary>
    /// Aggregated counters for resource tree
    /// </summary>
    Aggregated = Ydb.Coordination.RateLimiterCountersMode.Aggregated,

    /// <summary>
    /// Counters on every resource
    /// </summary>
    Detailed = Ydb.Coordination.RateLimiterCountersMode.Detailed
}
