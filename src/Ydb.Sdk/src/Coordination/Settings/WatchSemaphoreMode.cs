namespace Ydb.Sdk.Coordination.Settings;

public enum WatchSemaphoreMode
{
    // Watch for changes in semaphore data
    WatchData,

    // Watch for changes in semaphore owners
    WatchOwners,

    // Watch for changes in semaphore data or owners
    WatchDataAndOwners
}
