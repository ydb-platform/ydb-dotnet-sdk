namespace Ydb.Sdk.Coordination.Settings;

public enum WatchSemaphoreMode
{
    /// <summary>
    /// Watch for changes in semaphore data.
    /// </summary>
    WatchData,

    /// <summary>
    /// Watch for changes in semaphore owners.
    /// </summary>
    WatchOwners,

    /// <summary>
    /// Watch for changes in semaphore data or owners.
    /// </summary>
    WatchDataAndOwners
}
