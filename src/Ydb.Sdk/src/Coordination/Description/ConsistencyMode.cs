namespace Ydb.Sdk.Coordination.Description;

public enum ConsistencyMode
{
    /// <summary>
    /// The default or current value.
    /// </summary>
    Unset = Ydb.Coordination.ConsistencyMode.Unset,

    /// <summary>
    /// Strict mode makes sure operations may only complete on current leader.
    /// </summary>
    Strict = Ydb.Coordination.ConsistencyMode.Strict,

    /// <summary>
    /// Relaxed mode allows operations to complete on stale masters.
    /// </summary>
    Relaxed = Ydb.Coordination.ConsistencyMode.Relaxed
}
