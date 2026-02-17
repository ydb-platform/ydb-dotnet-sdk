namespace Ydb.Sdk.Coordinator.Description;

public enum ConsistencyMode
{
    /// <summary>The default or current value</summary>
    Unset,

    /// <summary>Strict mode makes sure operations may only complete on current leader</summary>
    Strict,

    /// <summary>Relaxed mode allows operations to complete on stale masters</summary>
    Relaxed
}
