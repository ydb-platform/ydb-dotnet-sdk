namespace Ydb.Sdk.Coordinator.Description;

public enum RateLimiterCountersMode
{
    /// <summary>The default or current value</summary>
    Unset,

    /// <summary>Aggregated counters for resource tree</summary>
    Aggregated,

    /// <summary>Counters on every resource</summary>
    Detailed
}
